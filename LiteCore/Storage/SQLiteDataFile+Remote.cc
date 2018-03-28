//
//  SQLiteDataFile+Remote.cc
//  LiteCore
//
//  Created by Jens Alfke on 3/27/18.
//Copyright © 2018 Couchbase. All rights reserved.
//

#include "SQLiteDataFile.hh"
#include "SQLite_Internal.hh"
#include "Error.hh"
#include "FilePath.hh"
#include "SQLiteCpp/SQLiteCpp.h"
#include "PlatformCompat.hh"

using namespace std;

namespace litecore {

    static inline alloc_slice asAllocSlice(const SQLite::Column &col) {
        return alloc_slice(col.getBlob(), col.getBytes());
    }


    void SQLiteDataFile::createRemotesTables() {
        _exec("BEGIN; "
              "CREATE TABLE remotes (remote_id INTEGER PRIMARY KEY, "
                                    "address TEXT UNIQUE);"
              "CREATE TABLE remote_revs (remote_id INTEGER FOREIGN KEY REFERENCES remotes, "
                                        "docID TEXT, "
                                        "version BLOB NOT NULL);"
              "PRAGMA user_version=201; "
              "END;");
    }

    DataFile::RemoteID SQLiteDataFile::getRemote(slice address, bool canCreate) {
        auto &stmt = compile(_getRemoteStmt, "SELECT remote_id FROM remotes WHERE address=?");
        stmt.bindNoCopy(1, (const char*)address.buf, (int)address.size);
        UsingStatement u(stmt);
        if (stmt.executeStep())
            return stmt.getColumn(0);

        if (!canCreate)
            return kNoRemoteID;

        SQLite::Statement insertStmt(*_sqlDb, "INSERT INTO remotes (address) VALUES (?)");
        insertStmt.bindNoCopy(1, (const char*)address.buf, (int)address.size);
        insertStmt.exec();
        return RemoteID(_sqlDb->getLastInsertRowid());
    }


    alloc_slice SQLiteDataFile::getRemoteAddress(RemoteID remote) {
        SQLite::Statement stmt(*_sqlDb,
                               "SELECT address FROM remotes WHERE remote_id=?");
        stmt.bind(1, remote);
        if (stmt.executeStep())
            return asAllocSlice(stmt.getColumn(0));
        return {};
    }


    alloc_slice SQLiteDataFile::latestRevisionOnRemote(RemoteID remote, slice docID) {
        auto &stmt = compile(_latestRevOnRemoteStmt,
                             "SELECT version FROM remote_revs WHERE remote_id=? AND docID=?");
        stmt.bind(1, remote);
        stmt.bindNoCopy(2, (char*)docID.buf, (int)docID.size);
        UsingStatement u(stmt);
        if (stmt.executeStep())
            return asAllocSlice(stmt.getColumn(0));
        return {};
    }


    void SQLiteDataFile::setLatestRevisionOnRemote(RemoteID remote, slice docID, slice revID)
    {
        auto &stmt = compile(_setLatestRevOnRemoteStmt,
                             "INSERT OR REPLACE INTO remote_revs (remote_id, docID, version) "
                             "VALUES (?, ?, ?)");
        stmt.bind(1, remote);
        stmt.bind(2, (const char*)docID.buf, (int)docID.size);
        stmt.bind(3, (const char*)revID.buf, (int)revID.size);
        UsingStatement u(stmt);
        stmt.exec();
    }

}
