//
//  c4DatabaseTest.cc
//  CBForest
//
//  Created by Jens Alfke on 9/14/15.
//  Copyright © 2015 Couchbase. All rights reserved.
//

//  Requires CppUnit <http://sourceforge.net/projects/cppunit/>,
//  which you can install using 'brew' or 'apt_get' or whatever.

#include "c4Test.hh"
#include "c4Private.h"
#include "c4DocEnumerator.h"
#include "c4ExpiryEnumerator.h"
#include <cmath>

#include "forestdb.h"
#include "sqlite3.h"

#ifdef _MSC_VER
#define random() rand()
#include <ctime>
#include "Windows.h"
#define sleep(sec) Sleep((sec)*1000)
#else
#include "unistd.h"
#endif

class C4DatabaseTest : public C4Test {
    public:

    void assertMessage(C4ErrorDomain domain, int code, const char *expectedMsg) {
        C4SliceResult msg = c4error_getMessage({domain, code});
        AssertEqual(std::string((char*)msg.buf, msg.size), std::string(expectedMsg));
        c4slice_free(msg);

        char buf[256];
        char *cmsg = c4error_getMessageC({domain, code}, buf, sizeof(buf));
        AssertEqual(std::string(cmsg), std::string(expectedMsg));
        AssertEqual(cmsg, &buf[0]);
    }

    void testErrorMessages() {
        C4SliceResult msg = c4error_getMessage({ForestDBDomain, 0});
        AssertEqual(msg.buf, (const void*)nullptr);
        AssertEqual((unsigned long)msg.size, 0ul);

        char buf[256];
        char *cmsg = c4error_getMessageC({ForestDBDomain, 0}, buf, sizeof(buf));
        AssertEqual(cmsg, &buf[0]);
        AssertEqual(buf[0], '\0');

        assertMessage(ForestDBDomain, FDB_RESULT_KEY_NOT_FOUND, "key not found");
        assertMessage(SQLiteDomain, SQLITE_CORRUPT, "database disk image is malformed");
        assertMessage(CBForestDomain, 15, "invalid parameter");
        assertMessage(POSIXDomain, ENOENT, "No such file or directory");
        assertMessage(CBForestDomain, kC4ErrorIndexBusy, "index busy; can't close view");
        assertMessage(ForestDBDomain, -1234, "unknown error");
        assertMessage((C4ErrorDomain)666, -1234, "unknown error domain");
    }

    void testTransaction() {
        AssertEqual(c4db_getDocumentCount(db), (C4SequenceNumber)0);
        Assert(!c4db_isInTransaction(db));
        C4Error(error);
        Assert(c4db_beginTransaction(db, &error));
        Assert(c4db_isInTransaction(db));
        Assert(c4db_beginTransaction(db, &error));
        Assert(c4db_isInTransaction(db));
        Assert(c4db_endTransaction(db, true, &error));
        Assert(c4db_isInTransaction(db));
        Assert(c4db_endTransaction(db, true, &error));
        Assert(!c4db_isInTransaction(db));
    }


    void testCreateRawDoc() {
        const C4Slice key = c4str("key");
        const C4Slice meta = c4str("meta");
        C4Error error;
        Assert(c4db_beginTransaction(db, &error));
        c4raw_put(db, c4str("test"), key, meta, kBody, &error);
        Assert(c4db_endTransaction(db, true, &error));

        C4RawDocument *doc = c4raw_get(db, c4str("test"), key, &error);
        Assert(doc != NULL);
        AssertEqual(doc->key, key);
        AssertEqual(doc->meta, meta);
        AssertEqual(doc->body, kBody);
        c4raw_free(doc);

        // Nonexistent:
        AssertEqual(c4raw_get(db, c4str("test"), c4str("bogus"), &error), (C4RawDocument*)nullptr);
        AssertEqual(error.domain, CBForestDomain);
        AssertEqual(error.code, (int)kC4ErrorNotFound);
}


    void testCreateVersionedDoc() {
        // Try reading doc with mustExist=true, which should fail:
        C4Error error;
        C4Document* doc;
        doc = c4doc_get(db, kDocID, true, &error);
        Assert(!doc);
        AssertEqual((uint32_t)error.domain, (uint32_t)CBForestDomain);
        AssertEqual(error.code, (int)kC4ErrorNotFound);
        c4doc_free(doc);

        // Now get the doc with mustExist=false, which returns an empty doc:
        doc = c4doc_get(db, kDocID, false, &error);
        Assert(doc != NULL);
        AssertEqual(doc->flags, (C4DocumentFlags)0);
        AssertEqual(doc->docID, kDocID);
        Assert(doc->revID.buf == 0);
        Assert(doc->selectedRev.revID.buf == 0);
        c4doc_free(doc);

        {
            TransactionHelper t(db);
            C4DocPutRequest rq = {};
            rq.existingRevision = true;
            rq.docID = kDocID;
            rq.history = &kRevID;
            rq.historyCount = 1;
            rq.body = kBody;
            rq.save = true;
            doc = c4doc_put(db, &rq, NULL, &error);
            Assert(doc != NULL);
            AssertEqual(doc->revID, kRevID);
            AssertEqual(doc->selectedRev.revID, kRevID);
            AssertEqual(doc->selectedRev.flags, (C4RevisionFlags)kRevLeaf);
            AssertEqual(doc->selectedRev.body, kBody);
            c4doc_free(doc);
        }

        // Reload the doc:
        doc = c4doc_get(db, kDocID, true, &error);
        Assert(doc != NULL);
        AssertEqual(doc->flags, (C4DocumentFlags)kExists);
        AssertEqual(doc->docID, kDocID);
        AssertEqual(doc->revID, kRevID);
        AssertEqual(doc->selectedRev.revID, kRevID);
        AssertEqual(doc->selectedRev.sequence, (C4SequenceNumber)1);
        AssertEqual(doc->selectedRev.body, kBody);
        c4doc_free(doc);

        // Get the doc by its sequence:
        doc = c4doc_getBySequence(db, 1, &error);
        Assert(doc != NULL);
        AssertEqual(doc->flags, (C4DocumentFlags)kExists);
        AssertEqual(doc->docID, kDocID);
        AssertEqual(doc->revID, kRevID);
        AssertEqual(doc->selectedRev.revID, kRevID);
        AssertEqual(doc->selectedRev.sequence, (C4SequenceNumber)1);
        AssertEqual(doc->selectedRev.body, kBody);
        c4doc_free(doc);
    }


    void testCreateMultipleRevisions() {
        const C4Slice kBody2 = C4STR("{\"ok\":\"go\"}");
        createRev(kDocID, kRevID, kBody);
        createRev(kDocID, kRev2ID, kBody2);
        createRev(kDocID, kRev2ID, kBody2, false); // test redundant insert

        // Reload the doc:
        C4Error error;
        C4Document *doc = c4doc_get(db, kDocID, true, &error);
        Assert(doc != NULL);
        AssertEqual(doc->flags, (C4DocumentFlags)kExists);
        AssertEqual(doc->docID, kDocID);
        AssertEqual(doc->revID, kRev2ID);
        AssertEqual(doc->selectedRev.revID, kRev2ID);
        AssertEqual(doc->selectedRev.sequence, (C4SequenceNumber)2);
        AssertEqual(doc->selectedRev.body, kBody2);

        if (schemaVersion() == 1) {
            // Select 1st revision:
            Assert(c4doc_selectParentRevision(doc));
            AssertEqual(doc->selectedRev.revID, kRevID);
            AssertEqual(doc->selectedRev.sequence, (C4SequenceNumber)1);
            AssertEqual(doc->selectedRev.body, kC4SliceNull);
            Assert(c4doc_hasRevisionBody(doc));
            Assert(c4doc_loadRevisionBody(doc, &error)); // have to explicitly load the body
            AssertEqual(doc->selectedRev.body, kBody);
            Assert(!c4doc_selectParentRevision(doc));
            c4doc_free(doc);

            // Compact database:
            Assert(c4db_compact(db, &error));

            doc = c4doc_get(db, kDocID, true, &error);
            Assert(doc != NULL);
            Assert(c4doc_selectParentRevision(doc));
            AssertEqual(doc->selectedRev.revID, kRevID);
            AssertEqual(doc->selectedRev.sequence, (C4SequenceNumber)1);
            if (!isSQLite()) {
                AssertEqual(doc->selectedRev.body, kC4SliceNull);
                Assert(!c4doc_hasRevisionBody(doc));
                Assert(!c4doc_loadRevisionBody(doc, &error));
            }

            // Purge doc
            {
                TransactionHelper t(db);
                int nPurged = c4doc_purgeRevision(doc, kRev2ID, &error);
                AssertEqual(nPurged, 2);
                Assert(c4doc_save(doc, 20, &error));
            }
        }
        c4doc_free(doc);
    }


    void testGetForPut() {
        C4Error error;
        TransactionHelper t(db);

        // Creating doc given ID:
        auto doc = c4doc_getForPut(db, kDocID, kC4SliceNull, false, false, &error);
        Assert(doc != NULL);
        AssertEqual(doc->docID, kDocID);
        AssertEqual(doc->revID, kC4SliceNull);
        AssertEqual(doc->flags, (C4DocumentFlags)0);
        AssertEqual(doc->selectedRev.revID, kC4SliceNull);
        c4doc_free(doc);

        // Creating doc, no ID:
        doc = c4doc_getForPut(db, kC4SliceNull, kC4SliceNull, false, false, &error);
        Assert(doc != NULL);
        Assert(doc->docID.size >= 20);  // Verify it got a random doc ID
        AssertEqual(doc->revID, kC4SliceNull);
        AssertEqual(doc->flags, (C4DocumentFlags)0);
        AssertEqual(doc->selectedRev.revID, kC4SliceNull);
        c4doc_free(doc);

        // Delete with no revID given
        doc = c4doc_getForPut(db, kDocID, kC4SliceNull, true/*deleting*/, false, &error);
        Assert(doc == NULL);
        AssertEqual(error.code, 404);

        // Adding new rev of nonexistent doc:
        doc = c4doc_getForPut(db, kDocID, kRevID, false, false, &error);
        Assert(doc == NULL);
        AssertEqual(error.code, 404);

        // Adding new rev of existing doc:
        createRev(kDocID, kRevID, kBody);
        doc = c4doc_getForPut(db, kDocID, kRevID, false, false, &error);
        Assert(doc != NULL);
        AssertEqual(doc->docID, kDocID);
        AssertEqual(doc->revID, kRevID);
        AssertEqual(doc->flags, (C4DocumentFlags)kExists);
        AssertEqual(doc->selectedRev.revID, kRevID);
        c4doc_free(doc);

        // Adding new rev, with nonexistent parent:
        doc = c4doc_getForPut(db, kDocID, kRev2ID, false, false, &error);
        Assert(doc == NULL);
        AssertEqual(error.code, 404);

        // Conflict -- try & fail to update non-current rev:
        const C4Slice kBody2 = C4STR("{\"ok\":\"go\"}");
        createRev(kDocID, kRev2ID, kBody2);
        doc = c4doc_getForPut(db, kDocID, kRevID, false, false, &error);
        Assert(doc == NULL);
        AssertEqual(error.code, 409);

        // Conflict -- force an update of non-current rev:
        doc = c4doc_getForPut(db, kDocID, kRevID, false, true/*allowConflicts*/, &error);
        Assert(doc != NULL);
        AssertEqual(doc->docID, kDocID);
        AssertEqual(doc->selectedRev.revID, kRevID);
        c4doc_free(doc);

        // Deleting the doc:
        doc = c4doc_getForPut(db, kDocID, kRev2ID, true/*deleted*/, false, &error);
        Assert(doc != NULL);
        AssertEqual(doc->docID, kDocID);
        AssertEqual(doc->selectedRev.revID, kRev2ID);
        c4doc_free(doc);
        // Actually delete it:
        static const C4Slice kRev3ID= C4STR("3-deadbeef");
        createRev(kDocID, kRev3ID, kC4SliceNull);

        // Re-creating the doc (no revID given):
        doc = c4doc_getForPut(db, kDocID, kC4SliceNull, false, false, &error);
        Assert(doc != NULL);
        AssertEqual(doc->docID, kDocID);
        AssertEqual(doc->revID, kRev3ID);
        AssertEqual(doc->flags, (C4DocumentFlags)(kExists | kDeleted));
        AssertEqual(doc->selectedRev.revID, kRev3ID);
        c4doc_free(doc);
    }


    void testPut() {
        C4Error error;
        TransactionHelper t(db);

        // Creating doc given ID:
        C4DocPutRequest rq = {};
        rq.docID = kDocID;
        rq.body = kBody;
        rq.save = true;
        auto doc = c4doc_put(db, &rq, NULL, &error);
        Assert(doc != NULL);
        AssertEqual(doc->docID, kDocID);
        C4Slice kExpectedRevID = C4STR("1-c10c25442d9fe14fa3ca0db4322d7f1e43140fab");
        AssertEqual(doc->revID, kExpectedRevID);
        AssertEqual(doc->flags, (C4DocumentFlags)kExists);
        AssertEqual(doc->selectedRev.revID, kExpectedRevID);
        c4doc_free(doc);

        // Update doc:
        rq.body = C4STR("{\"ok\":\"go\"}");
        rq.history = &kExpectedRevID;
        rq.historyCount = 1;
        size_t commonAncestorIndex;
        doc = c4doc_put(db, &rq, &commonAncestorIndex, &error);
        Assert(doc != NULL);
        AssertEqual((unsigned long)commonAncestorIndex, 1ul);
        C4Slice kExpectedRev2ID = C4STR("2-32c711b29ea3297e27f3c28c8b066a68e1bb3f7b");
        AssertEqual(doc->revID, kExpectedRev2ID);
        AssertEqual(doc->flags, (C4DocumentFlags)kExists);
        AssertEqual(doc->selectedRev.revID, kExpectedRev2ID);
        c4doc_free(doc);

        // Insert existing rev:
        rq.body = C4STR("{\"from\":\"elsewhere\"}");
        rq.existingRevision = true;
        C4Slice history[2] = {kRev2ID, kExpectedRevID};
        rq.history = history;
        rq.historyCount = 2;
        doc = c4doc_put(db, &rq, &commonAncestorIndex, &error);
        Assert(doc != NULL);
        AssertEqual((unsigned long)commonAncestorIndex, 1ul);
        AssertEqual(doc->revID, kRev2ID);
        AssertEqual(doc->flags, (C4DocumentFlags)(kExists | kConflicted));
        AssertEqual(doc->selectedRev.revID, kRev2ID);
        c4doc_free(doc);
    }


    void setupAllDocs() {
        char docID[20];
        for (int i = 1; i < 100; i++) {
            sprintf(docID, "doc-%03d", i);
            createRev(c4str(docID), kRevID, kBody);
        }
        // Add a deleted doc to make sure it's skipped by default:
        createRev(c4str("doc-005DEL"), kRevID, kC4SliceNull);
    }


    void testAllDocs() {
        setupAllDocs();
        C4Error error;
        C4DocEnumerator* e;

        AssertEqual(c4db_getDocumentCount(db), 99ull);

        // No start or end ID:
        C4EnumeratorOptions options = kC4DefaultEnumeratorOptions;
        options.flags &= ~kC4IncludeBodies;
        e = c4db_enumerateAllDocs(db, kC4SliceNull, kC4SliceNull, &options, &error);
        Assert(e);
        char docID[20];
        int i = 1;
        while (c4enum_next(e, &error)) {
            auto doc = c4enum_getDocument(e, &error);
            Assert(doc);
            sprintf(docID, "doc-%03d", i);
            AssertEqual(doc->docID, c4str(docID));
            AssertEqual(doc->revID, kRevID);
            AssertEqual(doc->selectedRev.revID, kRevID);
            AssertEqual(doc->selectedRev.sequence, (C4SequenceNumber)i);
            AssertEqual(doc->selectedRev.body, kC4SliceNull);
            // Doc was loaded without its body, but it should load on demand:
            Assert(c4doc_loadRevisionBody(doc, &error)); // have to explicitly load the body
            AssertEqual(doc->selectedRev.body, kBody);

            C4DocumentInfo info;
            Assert(c4enum_getDocumentInfo(e, &info));
            AssertEqual(info.docID, c4str(docID));
            AssertEqual(doc->revID, kRevID);

            c4doc_free(doc);
            i++;
        }
        c4enum_free(e);
        AssertEqual(i, 100);

        // Start and end ID:
        e = c4db_enumerateAllDocs(db, c4str("doc-007"), c4str("doc-090"), NULL, &error);
        Assert(e);
        i = 7;
        while (c4enum_next(e, &error)) {
            auto doc = c4enum_getDocument(e, &error);
            AssertEqual(error.code, 0);
            Assert(doc);
            sprintf(docID, "doc-%03d", i);
            AssertEqual(doc->docID, c4str(docID));
            c4doc_free(doc);
            i++;
        }
        c4enum_free(e);
        AssertEqual(i, 91);

        // Some docs, by ID:
        options = kC4DefaultEnumeratorOptions;
        options.flags |= kC4IncludeDeleted;
        C4Slice docIDs[4] = {C4STR("doc-042"), C4STR("doc-007"), C4STR("bogus"), C4STR("doc-001")};
        e = c4db_enumerateSomeDocs(db, docIDs, 4, &options, &error);
        Assert(e);
        i = 0;
        while (c4enum_next(e, &error)) {
            auto doc = c4enum_getDocument(e, &error);
            AssertEqual(error.code, 0);
            Assert(doc);
            AssertEqual(doc->docID, docIDs[i]);
            AssertEqual(doc->sequence != 0, i != 2);
            c4doc_free(doc);
            i++;
        }
        c4enum_free(e);
        AssertEqual(i, 4);
    }


    void testAllDocsIncludeDeleted() {
        char docID[20];
        setupAllDocs();

        C4Error error;
        C4DocEnumerator* e;

        C4EnumeratorOptions options = kC4DefaultEnumeratorOptions;
        options.flags |= kC4IncludeDeleted;
        e = c4db_enumerateAllDocs(db, c4str("doc-004"), c4str("doc-007"), &options, &error);
        Assert(e);
        int i = 4;
        while (c4enum_next(e, &error)) {
            auto doc = c4enum_getDocument(e, &error);
            Assert(doc);
            if (i == 6)
                strcpy(docID, "doc-005DEL");
            else
                sprintf(docID, "doc-%03d", i - (i>=6));
            AssertEqual(doc->docID, c4str(docID));
            c4doc_free(doc);
            i++;
        }
        c4enum_free(e);
        AssertEqual(i, 9);
    }


    void testAllDocsInfo() {
        setupAllDocs();
        C4Error error;
        C4DocEnumerator* e;

        C4EnumeratorOptions options = kC4DefaultEnumeratorOptions;
        e = c4db_enumerateAllDocs(db, kC4SliceNull, kC4SliceNull, &options, &error);
        Assert(e);
        int i = 1;
        while(c4enum_next(e, &error)) {
            C4DocumentInfo doc;
            Assert(c4enum_getDocumentInfo(e, &doc));
            char docID[20];
            sprintf(docID, "doc-%03d", i);
            AssertEqual(doc.docID, c4str(docID));
            AssertEqual(doc.revID, kRevID);
            AssertEqual(doc.sequence, (uint64_t)i);
            AssertEqual(doc.flags, (C4DocumentFlags)kExists);
            i++;
        }
        c4enum_free(e);
        AssertEqual(error.code, 0);
        AssertEqual(i, 100);
    }


    void testChanges() {
        char docID[20];
        for (int i = 1; i < 100; i++) {
            sprintf(docID, "doc-%03d", i);
            createRev(c4str(docID), kRevID, kBody);
        }

        C4Error error;
        C4DocEnumerator* e;
        C4Document* doc;

        // Since start:
        C4EnumeratorOptions options = kC4DefaultEnumeratorOptions;
        options.flags &= ~kC4IncludeBodies;
        e = c4db_enumerateChanges(db, 0, &options, &error);
        Assert(e);
        C4SequenceNumber seq = 1;
        while (NULL != (doc = c4enum_nextDocument(e, &error))) {
            AssertEqual(doc->selectedRev.sequence, seq);
            sprintf(docID, "doc-%03llu", seq);
            AssertEqual(doc->docID, c4str(docID));
            c4doc_free(doc);
            seq++;
        }
        c4enum_free(e);

        // Since 6:
        e = c4db_enumerateChanges(db, 6, &options, &error);
        Assert(e);
        seq = 7;
        while (NULL != (doc = c4enum_nextDocument(e, &error))) {
            AssertEqual(doc->selectedRev.sequence, seq);
            sprintf(docID, "doc-%03llu", seq);
            AssertEqual(doc->docID, c4str(docID));
            c4doc_free(doc);
            seq++;
        }
        c4enum_free(e);
        AssertEqual(seq, (C4SequenceNumber)100);
    }

    void testExpired() {
        C4Slice docID = C4STR("expire_me");
        createRev(docID, kRevID, kBody);
        time_t expire = time(NULL) + 1;
        C4Error err;
        Assert(c4doc_setExpiration(db, docID, expire, &err));
        
        expire = time(NULL) + 2;
        // Make sure setting it to the same is also true
        Assert(c4doc_setExpiration(db, docID, expire, &err));
        Assert(c4doc_setExpiration(db, docID, expire, &err));
        
        C4Slice docID2 = C4STR("expire_me_too");
        createRev(docID2, kRevID, kBody);
        Assert(c4doc_setExpiration(db, docID2, expire, &err));
    
        C4Slice docID3 = C4STR("dont_expire_me");
        createRev(docID3, kRevID, kBody);
        sleep(2u);
        
        C4ExpiryEnumerator *e = c4db_enumerateExpired(db, &err);
        Assert(e != NULL);
        
        int expiredCount = 0;
        while(c4exp_next(e, NULL)) {
            C4Slice existingDocID = c4exp_getDocID(e);
            Assert(!c4SliceEqual(existingDocID, docID3));
            c4slice_free(existingDocID);
            expiredCount++;
        }
        
        c4exp_free(e);
        AssertEqual(expiredCount, 2);
        AssertEqual(c4doc_getExpiration(db, docID), (uint64_t)expire);
        AssertEqual(c4doc_getExpiration(db, docID2), (uint64_t)expire);
        AssertEqual(c4db_nextDocExpiration(db), (uint64_t)expire);
        
        e = c4db_enumerateExpired(db, &err);
        Assert(e != NULL);
        
        expiredCount = 0;
        while(c4exp_next(e, NULL)) {
            C4Slice existingDocID = c4exp_getDocID(e);
            Assert(!c4SliceEqual(existingDocID, docID3));
            c4slice_free(existingDocID);
            expiredCount++;
        }
        
        Assert(c4exp_purgeExpired(e, &err));
        c4exp_free(e);
        AssertEqual(expiredCount, 2);
        
        e = c4db_enumerateExpired(db, &err);
        Assert(e != NULL);
        
        expiredCount = 0;
        while(c4exp_next(e, NULL)) {
            expiredCount++;
        }
        
        Assert(c4exp_purgeExpired(e, &err));
        c4exp_free(e);
        AssertEqual(expiredCount, 0);
    }
    
    void testCancelExpire()
    {
        C4Slice docID = C4STR("expire_me");
        createRev(docID, kRevID, kBody);
        time_t expire = time(NULL) + 2;
        C4Error err;
        Assert(c4doc_setExpiration(db, docID, expire, &err));
        Assert(c4doc_setExpiration(db, docID, UINT64_MAX, &err));
        
        sleep(2u);
        auto e = c4db_enumerateExpired(db, &err);
        Assert(e != NULL);
        
        int expiredCount = 0;
        while(c4exp_next(e, NULL)) {
            expiredCount++;
        }
        
        Assert(c4exp_purgeExpired(e, &err));
        c4exp_free(e);
        AssertEqual(expiredCount, 0);
    }

    CPPUNIT_TEST_SUITE( C4DatabaseTest );
    CPPUNIT_TEST( testErrorMessages );
    CPPUNIT_TEST( testTransaction );
    CPPUNIT_TEST( testCreateRawDoc );
    CPPUNIT_TEST( testCreateVersionedDoc );
    CPPUNIT_TEST( testCreateMultipleRevisions );
    CPPUNIT_TEST( testAllDocs );
    CPPUNIT_TEST( testAllDocsInfo );
    CPPUNIT_TEST( testAllDocsIncludeDeleted );
    CPPUNIT_TEST( testChanges );
    CPPUNIT_TEST( testExpired );
    CPPUNIT_TEST( testCancelExpire );
    CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(C4DatabaseTest);



class C4VersionVectorDatabaseTest : public C4DatabaseTest {

    virtual int schemaVersion() const override     {return 2;}

    CPPUNIT_TEST_SUB_SUITE( C4VersionVectorDatabaseTest, C4DatabaseTest );
    CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(C4VersionVectorDatabaseTest);



class C4SQLiteDatabaseTest : public C4DatabaseTest {

    virtual const char* storageType() const override     {return kC4SQLiteStorageEngine;}

    CPPUNIT_TEST_SUB_SUITE( C4SQLiteDatabaseTest, C4DatabaseTest );
    CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(C4SQLiteDatabaseTest);



class C4EncryptedDatabaseTest : public C4DatabaseTest {

    static C4EncryptionKey sKey;

    const C4EncryptionKey* encryptionKey()  {
        if (!isForestDB())
            return nullptr; //FIX: Remove this once SQLCipher is supported
        sKey.algorithm = kC4EncryptionAES256;
        memcpy(sKey.bytes, "this is not a random key at all...", 32);
        return &sKey;
    }


    void testRekey() {
        testCreateRawDoc();

        C4Error error;
        Assert(c4db_rekey(db, NULL, &error));

        const C4Slice key = c4str("key");
        C4RawDocument *doc = c4raw_get(db, c4str("test"), key, &error);
        Assert(doc != NULL);
    }


    CPPUNIT_TEST_SUB_SUITE( C4EncryptedDatabaseTest, C4DatabaseTest );
    CPPUNIT_TEST( testRekey );
    CPPUNIT_TEST_SUITE_END();
};

C4EncryptionKey C4EncryptedDatabaseTest::sKey;

CPPUNIT_TEST_SUITE_REGISTRATION(C4EncryptedDatabaseTest);
