////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include "realm_coordinator.hpp"

#include "async_query.hpp"
#include "cached_realm.hpp"
#include "external_commit_helper.hpp"
#include "object_store.hpp"
#include "transact_log_handler.hpp"

#include <realm/commit_log.hpp>
#include <realm/group_shared.hpp>
#include <realm/lang_bind_helper.hpp>
#include <realm/query.hpp>
#include <realm/table_view.hpp>

#include <cassert>

using namespace realm;
using namespace realm::_impl;

static std::mutex s_coordinator_mutex;
static std::map<std::string, std::weak_ptr<RealmCoordinator>> s_coordinators_per_path;

std::shared_ptr<RealmCoordinator> RealmCoordinator::get_coordinator(StringData path)
{
    std::lock_guard<std::mutex> lock(s_coordinator_mutex);
    std::shared_ptr<RealmCoordinator> coordinator;

    auto it = s_coordinators_per_path.find(path);
    if (it != s_coordinators_per_path.end()) {
        coordinator = it->second.lock();
    }

    if (!coordinator) {
        s_coordinators_per_path[path] = coordinator = std::make_shared<RealmCoordinator>();
    }

    return coordinator;
}

std::shared_ptr<RealmCoordinator> RealmCoordinator::get_existing_coordinator(StringData path)
{
    std::lock_guard<std::mutex> lock(s_coordinator_mutex);
    auto it = s_coordinators_per_path.find(path);
    return it == s_coordinators_per_path.end() ? nullptr : it->second.lock();
}

std::shared_ptr<Realm> RealmCoordinator::get_realm(Realm::Config config)
{
    std::lock_guard<std::mutex> lock(m_realm_mutex);
    if (!m_notifier) {
        m_config = config;
        m_notifier = std::make_unique<ExternalCommitHelper>(*this);
    }
    else {
        if (m_config.read_only != config.read_only) {
            throw MismatchedConfigException("Realm at path already opened with different read permissions.");
        }
        if (m_config.in_memory != config.in_memory) {
            throw MismatchedConfigException("Realm at path already opened with different inMemory settings.");
        }
        if (m_config.encryption_key != config.encryption_key) {
            throw MismatchedConfigException("Realm at path already opened with a different encryption key.");
        }
        if (m_config.schema_version != config.schema_version && config.schema_version != ObjectStore::NotVersioned) {
            throw MismatchedConfigException("Realm at path already opened with different schema version.");
        }
        // FIXME - enable schma comparison
        if (/* DISABLES CODE */ (false) && m_config.schema != config.schema) {
            throw MismatchedConfigException("Realm at path already opened with different schema");
        }
    }

    if (config.cache) {
        for (auto& cachedRealm : m_cached_realms) {
            if (cachedRealm.is_cached_for_current_thread()) {
                // can be null if we jumped in between ref count hitting zero and
                // unregister_realm() getting the lock
                if (auto realm = cachedRealm.realm()) {
                    return realm;
                }
            }
        }
    }

    auto realm = std::make_shared<Realm>(config);
    realm->init(shared_from_this());
    m_cached_realms.emplace_back(realm, m_config.cache);
    return realm;
}

const Schema* RealmCoordinator::get_schema() const noexcept
{
    // FIXME: this is not thread-safe
    return m_cached_realms.empty() ? nullptr : m_config.schema.get();
}

RealmCoordinator::RealmCoordinator() = default;

RealmCoordinator::~RealmCoordinator()
{
    std::lock_guard<std::mutex> coordinator_lock(s_coordinator_mutex);
    for (auto it = s_coordinators_per_path.begin(); it != s_coordinators_per_path.end(); ) {
        if (it->second.expired()) {
            it = s_coordinators_per_path.erase(it);
        }
        else {
            ++it;
        }
    }
}

void RealmCoordinator::unregister_realm(Realm*)
{
    std::lock_guard<std::mutex> lock(m_realm_mutex);
    for (size_t i = 0; i < m_cached_realms.size(); ++i) {
        if (m_cached_realms[i].expired()) {
            if (i + 1 < m_cached_realms.size()) {
                m_cached_realms[i] = std::move(m_cached_realms.back());
            }
            m_cached_realms.pop_back();
        }
    }
}

void RealmCoordinator::clear_cache()
{
    std::lock_guard<std::mutex> lock(s_coordinator_mutex);
    s_coordinators_per_path.clear();
}

void RealmCoordinator::send_commit_notifications()
{
    m_notifier->notify_others();
}

void RealmCoordinator::pin_version(uint_fast64_t version, uint_fast32_t index)
{
    if (m_async_error) {
        return;
    }

    SharedGroup::VersionID versionid(version, index);
    if (!m_advancer_sg) {
        try {
            m_advancer_history = realm::make_client_history(m_config.path, m_config.encryption_key.data());
            auto durability = m_config.in_memory ? SharedGroup::durability_MemOnly : SharedGroup::durability_Full;
            m_advancer_sg = std::make_unique<SharedGroup>(*m_advancer_history, durability, m_config.encryption_key.data());
            m_advancer_sg->begin_read(versionid);
        }
        catch (...) {
            m_async_error = std::current_exception();
            m_advancer_sg = nullptr;
            m_advancer_history = nullptr;
        }
    }
    else if (m_queries.empty() && m_new_queries.empty()) {
        // If this is the first query then we don't already have a read transaction
        m_advancer_sg->begin_read(version);
    }
    else if (versionid < m_advancer_sg->get_version_of_current_transaction()) {
        // Ensure we're holding a readlock on the oldest version we have a
        // handover object for, as handover objects don't
        m_advancer_sg->end_read();
        m_advancer_sg->begin_read(version);
    }
}

AsyncQueryCancelationToken RealmCoordinator::register_query(const Results& r, Dispatcher dispatcher, std::function<void (Results, std::exception_ptr)> fn)
{
    return r.get_realm().m_coordinator->do_register_query(r, std::move(dispatcher), std::move(fn));
}

AsyncQueryCancelationToken RealmCoordinator::do_register_query(const Results& r, Dispatcher dispatcher, std::function<void (Results, std::exception_ptr)> fn)
{
    if (m_config.read_only) {
        throw "no async read only";
    }

    auto handover = r.get_realm().m_shared_group->export_for_handover(r.get_query(), ConstSourcePayload::Copy);
    auto version = handover->version;
    auto query = std::make_shared<AsyncQuery>(r.get_sort(),
                                              std::move(handover),
                                              std::move(dispatcher),
                                              std::move(fn),
                                              *this);

    {
        std::lock_guard<std::mutex> lock(m_query_mutex);
        pin_version(version.version, version.index);
        m_new_queries.push_back(query);
    }

    m_notifier->notify_others(); // FIXME: untangle this
    return query;
}

void RealmCoordinator::unregister_query(AsyncQuery& registration)
{
    registration.parent.do_unregister_query(registration);
}

void RealmCoordinator::do_unregister_query(AsyncQuery& registration)
{
    auto swap_remove = [&](auto& container) {
        auto it = std::find_if(container.begin(), container.end(),
                               [&](auto const& ptr) { return ptr.get() == &registration; });
        if (it != container.end()) {
            std::iter_swap(--container.end(), it);
            container.pop_back();
            return true;
        }
        return false;
    };

    std::lock_guard<std::mutex> lock(m_query_mutex);
    swap_remove(m_queries) || swap_remove(m_new_queries);

    if (m_queries.empty() && m_new_queries.empty()) {
        // Make sure we aren't holding on to read versions needlessly if there
        // are no queries left, but don't close them entirely as opening shared
        // groups is expensive
        if (m_advancer_sg) {
            m_advancer_sg->end_read();
        }
        if (m_query_sg) {
            m_query_sg->end_read();
        }
    }
}

void RealmCoordinator::on_change()
{
    run_async_queries();

    std::lock_guard<std::mutex> lock(m_realm_mutex);
    for (auto& realm : m_cached_realms) {
        realm.notify();
    }
}

void RealmCoordinator::run_async_queries()
{
    std::lock_guard<std::mutex> lock(m_query_mutex);

    if (m_queries.empty() && m_new_queries.empty()) {
        return;
    }

    if (!m_query_sg && !m_async_error) {
        try {
            m_query_history = realm::make_client_history(m_config.path, m_config.encryption_key.data());
            SharedGroup::DurabilityLevel durability = m_config.in_memory ? SharedGroup::durability_MemOnly : SharedGroup::durability_Full;
            m_query_sg = std::make_unique<SharedGroup>(*m_query_history, durability, m_config.encryption_key.data());
            m_query_sg->begin_read();
        }
        catch (...) {
            // Store the error to be passed to the async queries
            m_async_error = std::current_exception();
            m_query_sg = nullptr;
            m_query_history = nullptr;
        }
    }

    if (m_async_error) {
        update_async_queries();
        return;
    }

    // Sort newly added queries by their source version so that we can pull them
    // all forward to the latest version in a single pass over the transaction log
    std::sort(m_new_queries.begin(), m_new_queries.end(), [](auto const& lft, auto const& rgt) {
        return lft->version() < rgt->version();
    });

    // Import all newly added queries to our helper SG
    for (auto& query : m_new_queries) {
        LangBindHelper::advance_read(*m_advancer_sg, *m_advancer_history, query->version());
        query->attach_to(*m_advancer_sg);
    }

    // Advance both SGs to the newest version
    LangBindHelper::advance_read(*m_advancer_sg, *m_advancer_history);
    LangBindHelper::advance_read(*m_query_sg, *m_query_history,
                                 m_advancer_sg->get_version_of_current_transaction());

    // Transfer all new queries over to the main SG
    for (auto& query : m_new_queries) {
        query->detatch();
        query->attach_to(*m_query_sg);
    }

    update_async_queries();

    m_advancer_sg->end_read();
    m_advancer_sg->begin_read(m_query_sg->get_version_of_current_transaction());
}

void RealmCoordinator::update_async_queries()
{
    // Move "new queries" to the main query list
    m_queries.reserve(m_queries.size() + m_new_queries.size());
    std::move(m_new_queries.begin(), m_new_queries.end(), std::back_inserter(m_queries));
    m_new_queries.clear();

    // Run each of the queries and send the updated results
    for (auto& query : m_queries) {
        if (m_async_error) {
            query->set_error(m_async_error);
        }
        else if (!query->update()) {
            continue;
        }
        if (query->get_mode() != AsyncQuery::Mode::Push) {
            continue;
        }

        std::weak_ptr<AsyncQuery> q = query;
        std::weak_ptr<RealmCoordinator> weak_self = shared_from_this();
        query->dispatch([q, weak_self] {
            auto self = weak_self.lock();
            auto query = q.lock();
            if (!query || !self) {
                return;
            }

            if (self->m_async_error) {
                query->deliver_error();
                unregister_query(*query);
                return;
            }

            SharedRealm realm = Realm::get_shared_realm(self->m_config);
            std::function<void()> fn;
            {
                std::lock_guard<std::mutex> lock(self->m_query_mutex);
                fn = query->get_results_for(realm, *realm->m_shared_group);
            }
            if (fn) {
                fn();
            }
        });
    }
}

namespace  {
std::vector<std::function<void()>> get_ready_queries_for_thread(std::vector<std::shared_ptr<_impl::AsyncQuery>>& queries,
                                                                Realm& realm, SharedGroup& sg) {
    std::vector<std::function<void()>> ret;
    for (auto& query : queries) {
        if (query->get_mode() != AsyncQuery::Mode::Pull) {
            continue;
        }

        if (auto r = query->get_results_for(realm.shared_from_this(), sg)) {
            ret.push_back(std::move(r));
        }
    }
    return ret;
}
}

void RealmCoordinator::advance_to_ready(Realm& realm)
{
    std::vector<std::function<void()>> async_results;

    {
        std::lock_guard<std::mutex> lock(m_query_mutex);

        SharedGroup::VersionID version;
        for (auto& query : m_queries) {
            if (query->get_mode() == AsyncQuery::Mode::Pull) {
                version = query->version();
                if (version != SharedGroup::VersionID()) {
                    break;
                }
            }
        }

        // no untargeted async queries; just advance to latest
        if (version.version == 0) {
            transaction::advance(*realm.m_shared_group, *realm.m_history, realm.m_binding_context.get());
            return;
        }
        // async results are out of date; ignore
        else if (version < realm.m_shared_group->get_version_of_current_transaction()) {
            return;
        }

        transaction::advance(*realm.m_shared_group, *realm.m_history, realm.m_binding_context.get(), version);
        async_results = get_ready_queries_for_thread(m_queries, realm, *realm.m_shared_group);
    }

    for (auto& results : async_results) {
        results();
    }
}

void RealmCoordinator::process_available_async(Realm& realm)
{
    std::vector<std::function<void()>> async_results;

    {
        std::lock_guard<std::mutex> lock(m_query_mutex);
        async_results = get_ready_queries_for_thread(m_queries, realm, *realm.m_shared_group);
    }

    for (auto& results : async_results) {
        results();
    }
}
