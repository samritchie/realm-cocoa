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

#include "cached_realm.hpp"
#include "external_commit_helper.hpp"
#include "object_store.hpp"

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

void RealmCoordinator::on_change()
{
    std::lock_guard<std::mutex> lock(m_realm_mutex);
    for (auto& realm : m_cached_realms) {
        realm.notify();
    }
}
