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
}

- (void)testCancelSubscriptionWithPendingReadyResulsts {
    // This test relies on blocks being called in the order in which they are
    // added, which is an implementation detail that could change

    dispatch_queue_t queue = dispatch_queue_create("queue", 0);
    dispatch_semaphore_t sema = dispatch_semaphore_create(0);
    __block int firstBlockCalls = 0;
    __block int secondBlockCalls = 0;
    __block RLMNotificationToken *token;

    [IntObject.allObjects deliverOn:queue block:^(RLMResults *results, NSError * _Nullable e) {
        ++firstBlockCalls;
        if (firstBlockCalls == 2) {
            [token stop];
        }
        dispatch_semaphore_signal(sema);
    }];

    token = [IntObject.allObjects deliverOn:queue block:^(RLMResults *results, NSError * _Nullable e) {
        ++secondBlockCalls;
        dispatch_semaphore_signal(sema);
    }];

    dispatch_semaphore_wait(sema, DISPATCH_TIME_FOREVER);
    dispatch_semaphore_wait(sema, DISPATCH_TIME_FOREVER);

    [RLMRealm.defaultRealm transactionWithBlock:^{
        [IntObject createInDefaultRealmWithValue:@[@0]];
    } error:nil];

    dispatch_semaphore_wait(sema, DISPATCH_TIME_FOREVER);
    dispatch_sync(queue, ^{});

    XCTAssertEqual(2, firstBlockCalls);
    XCTAssertEqual(1, secondBlockCalls);
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

@end
