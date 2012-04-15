/*
 * Copyright 2012 Yahoo! Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This is a implementation of the mapkeeper interface that uses 
 * kyoto cabinet.
 */
#include <kchashdb.h>
#include <iostream>
#include <cstdio>
#include "MapKeeper.h"
#include <boost/program_options.hpp>
#include <boost/ptr_container/ptr_map.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/filesystem.hpp>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
#include <arpa/inet.h>

#include <protocol/TBinaryProtocol.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>


using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

using namespace kyotocabinet;
using boost::shared_ptr;
using namespace boost::filesystem;
namespace po = boost::program_options;

bool syncmode;
using namespace mapkeeper;

class KyotoCabinetServer: virtual public MapKeeperIf {
public:
    KyotoCabinetServer(const std::string& directoryName) :
        directoryName_(directoryName) {

        // open all the existing databases
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        directory_iterator end_itr;
        for (directory_iterator itr(directoryName); itr != end_itr; itr++) {
            if (is_regular_file(itr->status())) {
                std::string mapName = itr->path().leaf().string();
                std::string fileName = itr->path().string();

                // skip hidden files
                if (mapName[0] == '.') {
                  continue;
                }

                TreeDB* db = new TreeDB();
                if (!db->open(fileName, BasicDB::OWRITER)) {
                    printf("ERROR: failed to open '%s'\n", fileName.c_str());
                    exit(1);
                }
                maps_.insert(mapName, db);
                printf("INFO: opened '%s'\n", fileName.c_str());
            }
        }
    }

    ResponseCode::type ping() {
        return ResponseCode::Success;
    }

    ResponseCode::type addMap(const std::string& mapName) {
        boost::unique_lock< boost::shared_mutex> writeLock(mutex_);;
        boost::ptr_map<std::string, TreeDB>::iterator itr = maps_.find(mapName);
        if (itr != maps_.end()) {
            return ResponseCode::MapExists;
        }
        TreeDB* db = new TreeDB();
        if (!db->open(directoryName_ + "/" + mapName,  BasicDB::OWRITER | BasicDB::OCREATE)) {
          return ResponseCode::Error;
        }
        std::string mapName_ = mapName;
        maps_.insert(mapName_, db);
        return ResponseCode::Success;
    }

    ResponseCode::type dropMap(const std::string& mapName) {
        boost::unique_lock< boost::shared_mutex> writeLock(mutex_);;
        boost::ptr_map<std::string, TreeDB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        if (!itr->second->close()) {
          return ResponseCode::Error;
        }
        maps_.erase(itr);
        // TODO error check. portable code.
        unlink((directoryName_ + "/" + mapName).c_str());
        return ResponseCode::Success;
    }

    void listMaps(StringListResponse& _return) {
        DIR *dp;
        struct dirent *dirp;
        if((dp  = opendir(directoryName_.c_str())) == NULL) {
            _return.responseCode = ResponseCode::Success;
            return;
        }

        while ((dirp = readdir(dp)) != NULL) {
            _return.values.push_back(std::string(dirp->d_name));
        }
        closedir(dp);
        _return.responseCode = ResponseCode::Success;
    }

    void scan(RecordListResponse& _return, const std::string& mapName, const ScanOrder::type order,
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
    }

    void scanAscending(RecordListResponse& _return, std::map<std::string, std::string>& map,
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        _return.responseCode = ResponseCode::ScanEnded;
    }

    void scanDescending(RecordListResponse& _return, std::map<std::string, std::string>& map,
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        _return.responseCode = ResponseCode::ScanEnded;
    }

    void get(BinaryResponse& _return, const std::string& mapName, const std::string& key) {
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, TreeDB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            _return.responseCode = ResponseCode::MapNotFound;
            return;
        }
        if (!itr->second->get(key, &(_return.value))) {
            _return.responseCode = ResponseCode::RecordNotFound;
            return;
        }
        _return.responseCode = ResponseCode::Success;
    }

    ResponseCode::type put(const std::string& mapName, const std::string& key, const std::string& value) {
        std::string mapName_ = mapName;
        boost::ptr_map<std::string, TreeDB>::iterator itr;
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        itr = maps_.find(mapName_);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type insert(const std::string& mapName, const std::string& key, const std::string& value) {
        // TODO Get and Put should be within a same transaction
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, TreeDB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }

        // add() returns false if the record already exists, and
        // the record value is not modified.
        // http://fallabs.com/kyotocabinet/api/classkyotocabinet_1_1BasicDB.html#a330568e1a92d74bfbc38682cd8604462
        if (!itr->second->add(key, value)) {
            return ResponseCode::RecordExists;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type update(const std::string& mapName, const std::string& key, const std::string& value) {
        // TODO Get and Put should be within a same transaction
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, TreeDB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }

        // replace() returns false if the record doesn't exist, and no
        // new record is created.
        // http://fallabs.com/kyotocabinet/api/classkyotocabinet_1_1BasicDB.html#ac1b65f395e4be9e9ef14973f564e3a48
        if (!itr->second->replace(key, value)) {
            return ResponseCode::RecordNotFound;
        }
        return ResponseCode::Success;

    }

    ResponseCode::type remove(const std::string& mapName, const std::string& key) {
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, TreeDB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        if (!itr->second->remove(key)) {
            return ResponseCode::RecordNotFound;
        }
        return ResponseCode::Success;
    }

private:
    std::string directoryName_; // directory to store db files.
    boost::ptr_map<std::string, TreeDB> maps_;
    boost::shared_mutex mutex_; // protect map_
};

int main(int argc, char **argv) {
    int port;
    std::string dir;
    po::variables_map vm;
    po::options_description config("");
    config.add_options()
        ("help,h", "produce help message")
        ("sync,s", "synchronous writes")
        ("port,p", po::value<int>(&port)->default_value(9090), "port to listen to")
        ("datadir,d", po::value<std::string>(&dir)->default_value("data"), "data directory")
        ;
    po::options_description cmdline_options;
    cmdline_options.add(config);
    store(po::command_line_parser(argc, argv).options(cmdline_options).run(), vm);
    notify(vm);
    if (vm.count("help")) {
        std::cout << config << std::endl; 
        exit(0);
    }
    syncmode = vm.count("sync");
    shared_ptr<KyotoCabinetServer> handler(new KyotoCabinetServer(dir));
    shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    TThreadedServer server (processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    return 0;
}
