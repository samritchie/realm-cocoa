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

#ifndef REALM_COORDINATOR_HPP
#define REALM_COORDINATOR_HPP

#include "shared_realm.hpp"

#include <realm/string_data.hpp>

namespace realm {
class Results;
struct AsyncQueryCancelationToken;
class ClientHistory;
class SharedGroup;

using Dispatcher = std::function<void (std::function<void()>)>;

namespace _impl {
class AsyncQuery;
class CachedRealm;
class ExternalCommitHelper;

// RealmCoordinator manages the weak cache of Realm instances and communication
// between per-thread Realm instances for a given file
class RealmCoordinator : public std::enable_shared_from_this<RealmCoordinator> {
public:
    // Get the coordinator for the given path, creating it if neccesary
    static std::shared_ptr<RealmCoordinator> get_coordinator(StringData path);
    // Get the coordinator for the given path, or null if there is none
    static std::shared_ptr<RealmCoordinator> get_existing_coordinator(StringData path);

    // Get a thread-local shared Realm with the given configuration
    // If the Realm is already open on another thread, validates that the given
    // configuration is compatible with the existing one
    std::shared_ptr<Realm> get_realm(Realm::Config config);

    const Schema* get_schema() const noexcept;
    uint64_t get_schema_version() const noexcept { return m_config.schema_version; }
    const std::string& get_path() const noexcept { return m_config.path; }

    // Asyncronously call notify() on every Realm instance for this coordinator's
    // path, including those in other processes
    void send_commit_notifications();

    // Clear the weak Realm cache for all paths
    // Should only be called in test code, as continuing to use the previously
    // cached instances will have odd results
    static void clear_cache();

    // Explicit constructor/destructor needed for the unique_ptrs to forward-declared types
    RealmCoordinator();
    ~RealmCoordinator();

    // Called by Realm's destructor to ensure the cache is cleaned up promptly
    // Do not call directly
    void unregister_realm(Realm* realm);

    // Called by m_notifier when there's a new commit to send notifications for
    void on_change();

    static AsyncQueryCancelationToken register_query(const Results& r, Dispatcher dispatcher, std::function<void (Results, std::exception_ptr)> fn);
    static void unregister_query(AsyncQuery& registration);

    // Advance the Realm to the most recent transaction version which all async
    // work is complete for
    void advance_to_ready(Realm& realm);
    void process_available_async(Realm& realm);

private:
    Realm::Config m_config;

    std::mutex m_realm_mutex;
    std::vector<CachedRealm> m_cached_realms;

    std::mutex m_query_mutex;
    std::vector<std::shared_ptr<_impl::AsyncQuery>> m_new_queries;
    std::vector<std::shared_ptr<_impl::AsyncQuery>> m_queries;

    // SharedGroup used for actually running async queries
    std::unique_ptr<ClientHistory> m_query_history;
    std::unique_ptr<SharedGroup> m_query_sg;

    // SharedGroup used to advance queries in m_new_queries to the main shared
    // group's transaction version
    std::unique_ptr<ClientHistory> m_advancer_history;
    std::unique_ptr<SharedGroup> m_advancer_sg;
    std::exception_ptr m_async_error;

    std::unique_ptr<_impl::ExternalCommitHelper> m_notifier;

    AsyncQueryCancelationToken do_register_query(const Results& r, Dispatcher dispatcher, std::function<void (Results, std::exception_ptr)> fn);
    void do_unregister_query(AsyncQuery& registration);

    // must be called with m_query_mutex locked
    void pin_version(uint_fast64_t version, uint_fast32_t index);
    void update_async_queries();

    void run_async_queries();
};

} // namespace _impl
} // namespace realm

#endif /* REALM_COORDINATOR_HPP */
