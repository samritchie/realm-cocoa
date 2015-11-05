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

#import "RLMTestCase.h"

#pragma clang diagnostic ignored "-Wunused-parameter"

// FIXME: test that things show up on the correct queue
// FIXME: test queue + main thread targets at same time

@interface AsyncTests : RLMTestCase
@end

@implementation AsyncTests
- (void)createObject:(int)value {
    @autoreleasepool {
        RLMRealm *realm = [RLMRealm defaultRealm];
        [realm transactionWithBlock:^{
            [IntObject createInDefaultRealmWithValue:@[@(value)]];
        }];
    }
}

- (void)testInitialResultsAreDelivered {
    [self createObject:1];

    XCTestExpectation *expectation = [self expectationWithDescription:@""];
    [[IntObject objectsWhere:@"intCol > 0"] deliverOnMainThread:^(RLMResults *results, NSError * _Nullable e) {
        XCTAssertEqual(results.count, 1U);
        [expectation fulfill];
    }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];
}

- (void)testNewResultsAreDeliveredAfterLocalCommit {
    __block XCTestExpectation *expectation = [self expectationWithDescription:@""];
    __block NSUInteger expected = 0;
    [[IntObject objectsWhere:@"intCol > 0"] deliverOnMainThread:^(RLMResults *results, NSError * _Nullable e) {
        XCTAssertEqual(results.count, expected++);
        [expectation fulfill];
    }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    expectation = [self expectationWithDescription:@""];
    [self createObject:1];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    expectation = [self expectationWithDescription:@""];
    [self createObject:2];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];
}

- (void)testNewResultsAreDeliveredAfterBackgroundCommit {
    __block XCTestExpectation *expectation = [self expectationWithDescription:@""];
    __block NSUInteger expected = 0;
    [[IntObject objectsWhere:@"intCol > 0"] deliverOnMainThread:^(RLMResults *results, NSError * _Nullable e) {
        XCTAssertEqual(results.count, expected++);
        [expectation fulfill];
    }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    expectation = [self expectationWithDescription:@""];
    [self dispatchAsyncAndWait:^{ [self createObject:1]; }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    expectation = [self expectationWithDescription:@""];
    [self dispatchAsyncAndWait:^{ [self createObject:2]; }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];
}

- (void)testResultsPerserveQuery {
    __block XCTestExpectation *expectation = [self expectationWithDescription:@""];
    __block NSUInteger expected = 0;
    [[IntObject objectsWhere:@"intCol > 0"] deliverOnMainThread:^(RLMResults *results, NSError * _Nullable e) {
        XCTAssertEqual(results.count, expected);
        [expectation fulfill];
    }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    expectation = [self expectationWithDescription:@""];
    ++expected;
    [self dispatchAsyncAndWait:^{ [self createObject:1]; }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    expectation = [self expectationWithDescription:@""];
    [self dispatchAsyncAndWait:^{ [self createObject:-1]; }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];
}

- (void)testResultsPerserveSort {
    __block XCTestExpectation *expectation = [self expectationWithDescription:@""];
    __block int expected = 0;
    [[IntObject.allObjects sortedResultsUsingProperty:@"intCol" ascending:NO] deliverOnMainThread:^(RLMResults *results, NSError * _Nullable e) {
        XCTAssertEqual([results.firstObject intCol], expected);
        [expectation fulfill];
    }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    expectation = [self expectationWithDescription:@""];
    expected = 1;
    [self dispatchAsyncAndWait:^{ [self createObject:1]; }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    expectation = [self expectationWithDescription:@""];
    [self dispatchAsyncAndWait:^{ [self createObject:-1]; }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    expectation = [self expectationWithDescription:@""];
    expected = 2;
    [self dispatchAsyncAndWait:^{ [self createObject:2]; }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];
}

- (void)testQueryingDeliveredQueryResults {
    __block XCTestExpectation *expectation = [self expectationWithDescription:@""];
    __block NSUInteger expected = 0;
    [[IntObject objectsWhere:@"intCol > 0"] deliverOnMainThread:^(RLMResults *results, NSError * _Nullable e) {
        XCTAssertEqual([results objectsWhere:@"intCol < 10"].count, expected++);
        [expectation fulfill];
    }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    expectation = [self expectationWithDescription:@""];
    [self dispatchAsyncAndWait:^{ [self createObject:1]; }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    expectation = [self expectationWithDescription:@""];
    [self dispatchAsyncAndWait:^{ [self createObject:2]; }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];
}

- (void)testQueryingDeliveredTableResults {
    __block XCTestExpectation *expectation = [self expectationWithDescription:@""];
    __block NSUInteger expected = 0;
    [[IntObject allObjects] deliverOnMainThread:^(RLMResults *results, NSError * _Nullable e) {
        XCTAssertEqual([results objectsWhere:@"intCol < 10"].count, expected++);
        [expectation fulfill];
    }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    expectation = [self expectationWithDescription:@""];
    [self dispatchAsyncAndWait:^{ [self createObject:1]; }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    expectation = [self expectationWithDescription:@""];
    [self dispatchAsyncAndWait:^{ [self createObject:2]; }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];
}

- (void)testQueryingDeliveredSortedResults {
    __block XCTestExpectation *expectation = [self expectationWithDescription:@""];
    __block int expected = 0;
    [[IntObject.allObjects sortedResultsUsingProperty:@"intCol" ascending:NO] deliverOnMainThread:^(RLMResults *results, NSError * _Nullable e) {
        XCTAssertEqual([[results objectsWhere:@"intCol < 10"].firstObject intCol], expected++);
        [expectation fulfill];
    }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    expectation = [self expectationWithDescription:@""];
    [self dispatchAsyncAndWait:^{ [self createObject:1]; }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    expectation = [self expectationWithDescription:@""];
    [self dispatchAsyncAndWait:^{ [self createObject:2]; }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];
}

- (void)testSortingDeliveredResults {
    __block XCTestExpectation *expectation = [self expectationWithDescription:@""];
    __block int expected = 0;
    [[IntObject allObjects] deliverOnMainThread:^(RLMResults *results, NSError * _Nullable e) {
        XCTAssertEqual([[results sortedResultsUsingProperty:@"intCol" ascending:NO].firstObject intCol], expected++);
        [expectation fulfill];
    }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    expectation = [self expectationWithDescription:@""];
    [self dispatchAsyncAndWait:^{ [self createObject:1]; }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    expectation = [self expectationWithDescription:@""];
    [self dispatchAsyncAndWait:^{ [self createObject:2]; }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];
}

- (void)testAutorefreshIsDelayedUntilResultsAreReady {
    // no idea how to test this...
}

- (void)testManualRefreshForcesBlockingUpdate {

}

- (void)testManualRefreshUsesAsyncResultsWhenPossible {
    __block bool called = false;
    [IntObject.allObjects deliverOnMainThread:^(RLMResults *results, NSError * _Nullable e) {
        called = true;
        XCTAssertNil(e);
        XCTAssertNotNil(results);
    }];

    [self dispatchAsyncAndWait:^{
        [RLMRealm.defaultRealm transactionWithBlock:^{}];
    }];

//    sleep(10);

    XCTAssertFalse(called);
    [RLMRealm.defaultRealm refresh];
    XCTAssertTrue(called);
}

- (void)testModifyingUnrelatedTableDoesNotTriggerResend {
    __block XCTestExpectation *expectation = [self expectationWithDescription:@""];
    [[IntObject allObjects] deliverOnMainThread:^(RLMResults *results, NSError * _Nullable e) {
        // will throw if called a second time
        [expectation fulfill];
    }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    [self waitForNotification:RLMRealmDidChangeNotification realm:RLMRealm.defaultRealm block:^{
        RLMRealm *realm = [RLMRealm defaultRealm];
        [realm transactionWithBlock:^{
            [StringObject createInDefaultRealmWithValue:@[@""]];
        }];
    }];
}

- (void)testDeliverToQueue {
    dispatch_queue_t queue = dispatch_queue_create("queue", 0);
    dispatch_semaphore_t sema = dispatch_semaphore_create(0);
    [[IntObject allObjects] deliverOn:queue block:^(RLMResults * _Nullable results, NSError * _Nullable err) {
        NSLog(@"%@ %@", results, err);
        dispatch_semaphore_signal(sema);
    }];
    dispatch_semaphore_wait(sema, DISPATCH_TIME_FOREVER);
    RLMRealm *realm = [RLMRealm defaultRealm];
    [realm transactionWithBlock:^{
        [IntObject createInDefaultRealmWithValue:@[@0]];
    }];
    dispatch_semaphore_wait(sema, DISPATCH_TIME_FOREVER);
    [realm transactionWithBlock:^{
        [IntObject createInDefaultRealmWithValue:@[@1]];
    }];
    dispatch_semaphore_wait(sema, DISPATCH_TIME_FOREVER);

    [[[IntObject allObjects] sortedResultsUsingProperty:@"intCol" ascending:NO] deliverOn:queue block:^(RLMResults * _Nullable results, NSError * _Nullable err) {
        NSLog(@"%@ %@", results, err);
        dispatch_semaphore_signal(sema);
    }];

    [realm transactionWithBlock:^{
        [IntObject createInDefaultRealmWithValue:@[@2]];
    }];
    dispatch_semaphore_wait(sema, DISPATCH_TIME_FOREVER);
    dispatch_semaphore_wait(sema, DISPATCH_TIME_FOREVER);

    dispatch_sync(queue, ^{ });
}

- (void)testStaleResultsAreNotDelivered {
    // This test relies on blocks being called in the order in which they are
    // added, which is an implementation detail that could change

    // This test sets up two async queries, waits for the initial results for
    // each to be ready, and then makes a commit to trigger both at the same
    // time. From the handler for the first, it then makes another commit, so
    // that the results for the second one are for an out-of-date transaction
    // version, and are not delivered.

    __block XCTestExpectation *exp1 = [self expectationWithDescription:@""];
    __block XCTestExpectation *exp2 = [self expectationWithDescription:@""];
    __block int firstBlockCalls = 0;
    __block int secondBlockCalls = 0;

    [IntObject.allObjects deliverOnMainThread:^(RLMResults *results, NSError * _Nullable e) {
        ++firstBlockCalls;
        if (firstBlockCalls == 1 || firstBlockCalls == 3) {
            [exp1 fulfill];
        }
        else {
            [results.realm beginWriteTransaction];
            [IntObject createInDefaultRealmWithValue:@[@1]];
            [results.realm commitWriteTransaction];
        }
    }];

    [IntObject.allObjects deliverOnMainThread:^(RLMResults *results, NSError * _Nullable e) {
        ++secondBlockCalls;
        [exp2 fulfill];
    }];

    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    exp1 = [self expectationWithDescription:@""];
    exp2 = [self expectationWithDescription:@""];

    [RLMRealm.defaultRealm transactionWithBlock:^{
        [IntObject createInDefaultRealmWithValue:@[@0]];
    } error:nil];

    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    XCTAssertEqual(3, firstBlockCalls);
    XCTAssertEqual(2, secondBlockCalls);
}

- (void)testDeliverToQueueVersionHandling {
    // This is the same as the above test, except using deliverToQueue

    dispatch_queue_t queue = dispatch_queue_create("queue", 0);
    dispatch_semaphore_t sema = dispatch_semaphore_create(0);
    __block int firstBlockCalls = 0;
    __block int secondBlockCalls = 0;

    [IntObject.allObjects deliverOn:queue block:^(RLMResults *results, NSError * _Nullable e) {
        ++firstBlockCalls;
        if (firstBlockCalls == 1 || firstBlockCalls == 3) {
            dispatch_semaphore_signal(sema);
        }
        else {
            [results.realm beginWriteTransaction];
            [IntObject createInDefaultRealmWithValue:@[@1]];
            [results.realm commitWriteTransaction];
        }
    }];

    [IntObject.allObjects deliverOn:queue block:^(RLMResults *results, NSError * _Nullable e) {
        ++secondBlockCalls;
        dispatch_semaphore_signal(sema);
    }];

    dispatch_semaphore_wait(sema, DISPATCH_TIME_FOREVER);
    dispatch_semaphore_wait(sema, DISPATCH_TIME_FOREVER);

    [RLMRealm.defaultRealm transactionWithBlock:^{
        [IntObject createInDefaultRealmWithValue:@[@0]];
    } error:nil];

    dispatch_semaphore_wait(sema, DISPATCH_TIME_FOREVER);
    dispatch_semaphore_wait(sema, DISPATCH_TIME_FOREVER);

    XCTAssertEqual(3, firstBlockCalls);
    XCTAssertEqual(2, secondBlockCalls);

    dispatch_sync(queue, ^{});
}

- (void)testCancelSubscriptionWithPendingReadyResulsts {
    // This test relies on the specific order in which things are called after
    // a commit, which is an implementation detail that could change

    dispatch_queue_t queue = dispatch_queue_create("queue", DISPATCH_QUEUE_SERIAL);
    dispatch_semaphore_t sema = dispatch_semaphore_create(0);

    // Create the async query and wait for the first run of it to complete
    __block int calls = 0;
    RLMNotificationToken *queryToken = [IntObject.allObjects deliverOn:queue block:^(RLMResults *results, NSError * _Nullable e) {
        ++calls;
        dispatch_semaphore_signal(sema);
    }];
    dispatch_semaphore_wait(sema, DISPATCH_TIME_FOREVER);

    // Block the queue which we've asked for results to be delivered on until
    // the main thread gets a commit notification, which happens only after
    // all async queries are ready
    dispatch_semaphore_t results_ready_sema = dispatch_semaphore_create(0);
    dispatch_async(queue , ^{
        dispatch_semaphore_signal(sema);
        dispatch_semaphore_wait(results_ready_sema, DISPATCH_TIME_FOREVER);

    });
    dispatch_semaphore_wait(sema, DISPATCH_TIME_FOREVER);

    [self waitForNotification:RLMRealmDidChangeNotification realm:RLMRealm.defaultRealm block:^{
        [RLMRealm.defaultRealm transactionWithBlock:^{
            [IntObject createInDefaultRealmWithValue:@[@0]];
        } error:nil];
    }];

    [queryToken stop];
    dispatch_semaphore_signal(results_ready_sema);

    dispatch_sync(queue, ^{});

    // Should have been called for the first run, but not after the commit
    XCTAssertEqual(1, calls);
}

- (void)testErrorHandling {
    RLMRealm *realm = [RLMRealm defaultRealm];

    // Force an error when opening the helper SharedGroups by deleting the file
    // after opening the Realm
    [NSFileManager.defaultManager removeItemAtPath:realm.path error:nil];

    __block bool called = false;
    XCTestExpectation *exp = [self expectationWithDescription:@""];
    [IntObject.allObjects deliverOnMainThread:^(RLMResults *results, NSError *error) {
        XCTAssertNil(results);
        XCTAssertNotNil(error);
        called = true;
        [exp fulfill];
    }];

    // Block should still be called asyncronously
    XCTAssertFalse(called);

    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    // Neither adding a new async query nor commiting a write transaction should
    // cause it to resend the error
    XCTestExpectation *exp2 = [self expectationWithDescription:@""];
    [IntObject.allObjects deliverOnMainThread:^(RLMResults *results, NSError *error) {
        XCTAssertNil(results);
        XCTAssertNotNil(error);
        [exp2 fulfill];
    }];
    [realm beginWriteTransaction];
    [IntObject createInDefaultRealmWithValue:@[@0]];
    [realm commitWriteTransaction];

    [self waitForExpectationsWithTimeout:2.0 handler:nil];
}

- (void)testQueueErrorHandling {
    RLMRealm *realm = [RLMRealm defaultRealm];

    // Force an error when opening the helper SharedGroups by deleting the file
    // after opening the Realm
    [NSFileManager.defaultManager removeItemAtPath:realm.path error:nil];

    dispatch_queue_t queue = dispatch_queue_create("queue", 0);
    dispatch_semaphore_t sema = dispatch_semaphore_create(0);
    __block bool called = false;
    [IntObject.allObjects deliverOn:queue block:^(RLMResults *results, NSError *error) {
        XCTAssertNil(results);
        XCTAssertNotNil(error);
        XCTAssertFalse(called);
        called = true;
        dispatch_semaphore_signal(sema);
    }];
    dispatch_semaphore_wait(sema, DISPATCH_TIME_FOREVER);

    // Neither adding a new async query nor commiting a write transaction should
    // cause it to resend the error
    [IntObject.allObjects deliverOn:queue block:^(RLMResults *results, NSError *error) {
        XCTAssertNil(results);
        XCTAssertNotNil(error);
        dispatch_semaphore_signal(sema);
    }];
    [realm beginWriteTransaction];
    [IntObject createInDefaultRealmWithValue:@[@0]];
    [realm commitWriteTransaction];

    dispatch_semaphore_wait(sema, DISPATCH_TIME_FOREVER);
    dispatch_sync(queue, ^{});
}

- (void)testRLMResultsInstanceIsReusedWhenRetainedExternally {
   __block RLMResults *prev;

    __block XCTestExpectation *expectation = [self expectationWithDescription:@""];
    __block bool first = true;
    [IntObject.allObjects deliverOnMainThread:^(RLMResults *results, NSError * _Nullable e) {
        if (first) {
            prev = results;
            first = false;
        }
        else {
            XCTAssertEqual(prev, results); // deliberately not EqualObjects
        }
        [expectation fulfill];
    }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    expectation = [self expectationWithDescription:@""];
    [self dispatchAsyncAndWait:^{ [self createObject:1]; }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];
}

- (void)testRLMResultsInstanceIsHeldWeakly {
   __weak __block RLMResults *prev;

    XCTestExpectation *expectation = [self expectationWithDescription:@""];
    [IntObject.allObjects deliverOnMainThread:^(RLMResults *results, NSError * _Nullable e) {
        XCTAssertNotNil(results);
        XCTAssertNil(e);
        prev = results;
        [expectation fulfill];
    }];
    [self waitForExpectationsWithTimeout:2.0 handler:nil];
    XCTAssertNil(prev);
}

- (void)testCancellationTokenKeepsSubscriptionAlive {
    __block XCTestExpectation *expectation = [self expectationWithDescription:@""];
    RLMNotificationToken *token;
    @autoreleasepool {
        token = [IntObject.allObjects deliverOnMainThread:^(RLMResults *results, NSError *err) {
            XCTAssertNotNil(results);
            XCTAssertNil(err);
            [expectation fulfill];
        }];
    }
    [self waitForExpectationsWithTimeout:2.0 handler:nil];

    // at this point there are no strong references to anything other than the
    // token, so verify that things haven't magically gone away
    // this would be better as a multi-process tests with the commit done
    // from a different process

    expectation = [self expectationWithDescription:@""];
    @autoreleasepool {
        [RLMRealm.defaultRealm transactionWithBlock:^{
            [IntObject createInDefaultRealmWithValue:@[@0]];
        }];
    }
    [self waitForExpectationsWithTimeout:2.0 handler:nil];
}

@end
