//
//  native_document.cpp
//  CBForest
//
//  Created by Jens Alfke on 9/11/15.
//  Copyright © 2015 Couchbase. All rights reserved.
//

#include "com_couchbase_cbforest_Document.h"
#include "native_glue.hh"
#include "c4Database.h"
#include <vector>

using namespace forestdb;
using namespace forestdb::jni;


static jfieldID kField_Handle;
static jfieldID kField_Flags;
static jfieldID kField_RevID;
static jfieldID kField_SelectedRevID;
static jfieldID kField_SelectedRevFlags;
static jfieldID kField_SelectedSequence;
static jfieldID kField_SelectedBody;


bool forestdb::jni::initDocument(JNIEnv *env) {
    jclass documentClass = env->FindClass("Document");
    if (!documentClass)
        return false;
    kField_Handle = env->GetFieldID(documentClass, "_handle", "J");
    kField_Flags = env->GetFieldID(documentClass, "_flags", "I");
    kField_RevID = env->GetFieldID(documentClass, "_revID", "Ljava/lang/String;");
    kField_SelectedRevID = env->GetFieldID(documentClass, "_selectedRevID", "Ljava/lang/String;");
    kField_SelectedRevFlags = env->GetFieldID(documentClass, "_selectedRevFlags", "I");
    kField_SelectedSequence = env->GetFieldID(documentClass, "_selectedSequence", "I");
    kField_SelectedBody = env->GetFieldID(documentClass, "_selectedBody", "[B");
    return kField_Handle && kField_Flags && kField_RevID && kField_SelectedRevID
        && kField_SelectedRevFlags && kField_SelectedSequence && kField_SelectedBody;
}


// Updates the _revID and _flags fields of the Java Document object
static void updateRevIDAndFlags
(JNIEnv *env, jobject self, C4Document *doc) {
    env->SetObjectField(self, kField_RevID, toJString(env, doc->revID));
    env->SetIntField   (self, kField_Flags, doc->flags);
}

// Updates the "_selectedXXXX" fields of the Java Document object
static void updateSelection
(JNIEnv *env, jobject self, C4Document *doc, bool withBody =false) {
    auto sel = &doc->selectedRev;
    env->SetObjectField(self, kField_SelectedRevID,    toJString(env, sel->revID));
    env->SetLongField  (self, kField_SelectedSequence, sel->sequence);
    env->SetIntField   (self, kField_SelectedRevFlags, sel->flags);
    env->SetObjectField(self, kField_SelectedBody,     toJByteArray(env, sel->body));
}


JNIEXPORT jlong JNICALL Java_com_couchbase_cbforest_Document_init
(JNIEnv *env, jobject self, jlong dbHandle, jstring jdocID, jboolean mustExist)
{
    jstringSlice docID(env, jdocID);
    C4Error error;
    C4Document *doc = c4doc_get((C4Database*)dbHandle, docID, mustExist, &error);
    if (!doc) {
        throwError(env, error);
        return 0;
    }
    updateRevIDAndFlags(env, self, doc);
    updateSelection(env, self, doc);
    return (jlong)doc;
}


JNIEXPORT jstring JNICALL Java_com_couchbase_cbforest_Document_initWithDocHandle
(JNIEnv *env, jobject self, jlong docHandle)
{
    auto doc = (C4Document*)docHandle;
    env->SetLongField(self, kField_Handle, docHandle);
    env->SetIntField(self, kField_Flags, doc->flags);
    updateRevIDAndFlags(env, self, doc);
    updateSelection(env, self, doc);
    return toJString(env, doc->docID);
}


JNIEXPORT void JNICALL Java_com_couchbase_cbforest_Document_free
(JNIEnv *env, jclass clazz, jlong docHandle)
{
    c4doc_free((C4Document*)docHandle);
}


JNIEXPORT jstring JNICALL Java_com_couchbase_cbforest_Document_getType
(JNIEnv *env, jclass clazz, jlong docHandle) {
    return toJString(env, c4doc_getType((C4Document*)docHandle));
}


JNIEXPORT void JNICALL Java_com_couchbase_cbforest_Document_setType
(JNIEnv *env, jclass clazz, jlong docHandle, jstring jtype)
{
    jstringSlice type(env, jtype);
    C4Error error;
    if (!c4doc_setType((C4Document*)docHandle, type, &error))
        throwError(env, error);
}



JNIEXPORT jboolean JNICALL Java_com_couchbase_cbforest_Document_selectRevID
(JNIEnv *env, jobject self, jstring jrevID, jboolean withBody)
{
    auto doc = (C4Document*)env->GetLongField(self, kField_Handle);
    jstringSlice revID(env, jrevID);
    C4Error error;
    bool ok = c4doc_selectRevision(doc, revID, withBody, &error);
    if (ok || error.domain == HTTPDomain)
        updateSelection(env, self, doc);
    else
        throwError(env, error);
    return ok;
}


JNIEXPORT jboolean JNICALL Java_com_couchbase_cbforest_Document_selectCurrentRev
(JNIEnv *env, jobject self)
{
    auto doc = (C4Document*)env->GetLongField(self, kField_Handle);
    bool ok = c4doc_selectCurrentRevision(doc);
    updateSelection(env, self, doc);
    return ok;
}


JNIEXPORT jboolean JNICALL Java_com_couchbase_cbforest_Document_selectParentRev
(JNIEnv *env, jobject self)
{
    auto doc = (C4Document*)env->GetLongField(self, kField_Handle);
    bool ok = c4doc_selectParentRevision(doc);
    updateSelection(env, self, doc);
    return ok;
}


JNIEXPORT jboolean JNICALL Java_com_couchbase_cbforest_Document_selectNextRev
(JNIEnv *env, jobject self)
{
    auto doc = (C4Document*)env->GetLongField(self, kField_Handle);
    bool ok = c4doc_selectNextRevision(doc);
    updateSelection(env, self, doc);
    return ok;
}


JNIEXPORT jboolean JNICALL Java_com_couchbase_cbforest_Document_selectNextLeaf
(JNIEnv *env, jobject self, jboolean includeDeleted, jboolean withBody)
{
    auto doc = (C4Document*)env->GetLongField(self, kField_Handle);
    C4Error error;
    bool ok = c4doc_selectNextLeafRevision(doc, includeDeleted, withBody, &error);
    if (ok || error.domain == HTTPDomain)  // 404 or 410 don't trigger exceptions
        updateSelection(env, self, doc, withBody);
    else
        throwError(env, error);
    return ok;
}


JNIEXPORT jbyteArray JNICALL Java_com_couchbase_cbforest_Document_readSelectedBody
(JNIEnv *env, jobject self)
{
    auto doc = (C4Document*)env->GetLongField(self, kField_Handle);
    C4Error error;
    if (!c4doc_loadRevisionBody(doc, &error)) {
        throwError(env, error);
        return NULL;
    }
    return toJByteArray(env, doc->selectedRev.body);
}


#pragma mark - INSERTING REVISIONS:


JNIEXPORT void JNICALL Java_com_couchbase_cbforest_Document_insertRevision 
(JNIEnv *env, jobject self,
 jstring jrevID, jbyteArray jbody,
 jboolean deleted, jboolean hasAtt,
 jboolean allowConflict)
{
    bool ok;
    C4Error error;
    {
        auto doc = (C4Document*)env->GetLongField(self, kField_Handle);
        jstringSlice revID(env, jrevID);
        jbyteArraySlice body(env, jbody, true); // critical
        ok = c4doc_insertRevision(doc, revID, body, deleted, hasAtt, allowConflict, &error);
    }
    if (!ok)
        throwError(env, error);
}


JNIEXPORT jint JNICALL Java_com_couchbase_cbforest_Document_insertRevisionWithHistory
(JNIEnv *env, jobject self,
 jstring jrevID, jbyteArray jbody,
 jboolean deleted, jboolean hasAtt,
 jobjectArray jhistory)
{
    auto doc = (C4Document*)env->GetLongField(self, kField_Handle);
    int inserted;
    C4Error error;
    {
        // Convert jhistory, a Java String[], to a C array of C4Slice:
        jsize n = env->GetArrayLength(jhistory);
        C4Slice history[n];
        std::vector<alloc_slice> historyAlloc;
        for (jsize i = 0; i < n; i++) {
            jbyteArray jitem = (jbyteArray) env->GetObjectArrayElement(jhistory, i);
            alloc_slice item = jbyteArraySlice::copy(env, jitem);
            historyAlloc.push_back(item); // so its memory won't be freed
            history[i] = (C4Slice){item.buf, item.size};
        }

        jstringSlice revID(env, jrevID);
        jbyteArraySlice body(env, jbody, true); // critical
        inserted = c4doc_insertRevisionWithHistory(doc, revID, body, deleted, hasAtt,
                                                   history, n,
                                                   &error);
    }
    if (inserted >= 0)
        updateRevIDAndFlags(env, self, doc);
    else
        throwError(env, error);
    return inserted;
}


JNIEXPORT void JNICALL Java_com_couchbase_cbforest_Document_save
(JNIEnv *env, jobject self, jint maxRevTreeDepth) {
    auto doc = (C4Document*)env->GetLongField(self, kField_Handle);
    C4Error error;
    if (c4doc_save(doc, maxRevTreeDepth, &error))
        updateRevIDAndFlags(env, self, doc);
    else
        throwError(env, error);
}
