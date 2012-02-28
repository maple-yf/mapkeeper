/**
 * This is a implementation of the mapkeeper interface that uses 
 * leveldb.
 *
 * http://leveldb.googlecode.com/svn/trunk/doc/index.html
 */
#include <iostream>
#include <cstdio>
#include "MapKeeper.h"
#include <leveldb/db.h>
#include <leveldb/cache.h>
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

using boost::shared_ptr;
using namespace boost::filesystem;
namespace po = boost::program_options;

using namespace mapkeeper;

int syncmode;
int blindinsert;
int blindupdate;
class LevelDbServer: virtual public MapKeeperIf {
public:
    LevelDbServer(const std::string& directoryName,
                  uint32_t writeBufferSizeMb, uint32_t blockCacheSizeMb) : 
        directoryName_(directoryName),
        writeBufferSizeMb_(writeBufferSizeMb),
        blockCacheSizeMb_(blockCacheSizeMb) {
        cache_ = leveldb::NewLRUCache(blockCacheSizeMb_ * 1024 * 1024);

        // open all the existing databases
        leveldb::DB* db;
        leveldb::Options options;
        options.create_if_missing = false;
        options.error_if_exists = false;
        options.write_buffer_size = writeBufferSizeMb_ * 1024 * 1024;
        options.block_cache = cache_;
        options.compression = leveldb::kNoCompression;

        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;

        directory_iterator end_itr;
        for (directory_iterator itr(directoryName); itr != end_itr;itr++) {
            if (is_directory(itr->status())) {
                std::string mapName = itr->path().filename().string();
                leveldb::Status status = leveldb::DB::Open(options, itr->path().string(), &db);
                assert(status.ok());
                maps_.insert(mapName, db);
            }
        }
    }

    ResponseCode::type ping() {
        return ResponseCode::Success;
    }

    ResponseCode::type addMap(const std::string& mapName) {
        leveldb::DB* db;
        leveldb::Options options;
        options.create_if_missing = true;
        options.error_if_exists = true;
        options.write_buffer_size = writeBufferSizeMb_ * 1024 * 1024;
        options.block_cache = cache_;
        leveldb::Status status = leveldb::DB::Open(options, directoryName_ + "/" + mapName, &db);
        if (!status.ok()) {
            // TODO check return code
            printf("status: %s\n", status.ToString().c_str());
            return ResponseCode::Error;
        }
        std::string mapName_ = mapName;
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        maps_.insert(mapName_, db);
        return ResponseCode::Success;
    }

    ResponseCode::type dropMap(const std::string& mapName) {
        std::string mapName_ = mapName;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr;
        boost::unique_lock< boost::shared_mutex> writeLock(mutex_);;
        itr = maps_.find(mapName_);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        maps_.erase(itr);
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
        boost::ptr_map<std::string, leveldb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            _return.responseCode = ResponseCode::MapNotFound;
            return;
        }
        leveldb::Status status = itr->second->Get(leveldb::ReadOptions(), key, &(_return.value));
        if (status.IsNotFound()) {
            _return.responseCode = ResponseCode::RecordNotFound;
            return;
        } else if (!status.ok()) {
            _return.responseCode = ResponseCode::Error;
            return;
        }
        _return.responseCode = ResponseCode::Success;
    }

    ResponseCode::type put(const std::string& mapName, const std::string& key, const std::string& value) {
        std::string mapName_ = mapName;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr;
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;

        itr = maps_.find(mapName_);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }

        leveldb::WriteOptions options;
        options.sync = syncmode ? true : false;
        leveldb::Status status = itr->second->Put(options, key, value);

        if (!status.ok()) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type insert(const std::string& mapName, const std::string& key, const std::string& value) {
        // TODO Get and Put should be within a same transaction
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
	if(!blindinsert) {
	  std::string recordValue;
	  leveldb::Status status = itr->second->Get(leveldb::ReadOptions(), key, &recordValue);
	  if (status.ok()) {
            printf("Record exists!\n");
            return ResponseCode::RecordExists;
	  } else if (!status.IsNotFound()) {
            return ResponseCode::Error;
	  }
	}
        leveldb::WriteOptions options;
        options.sync = syncmode ? true : false;
	leveldb::Status status = itr->second->Put(options, key, value);
        if (!status.ok()) {
            printf("insert not ok! %s\n", status.ToString().c_str());
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type update(const std::string& mapName, const std::string& key, const std::string& value) {
        // TODO Get and Put should be within a same transaction
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        std::string recordValue;
	if(!blindupdate) {
	  leveldb::Status status = itr->second->Get(leveldb::ReadOptions(), key, &recordValue);
	  if (status.IsNotFound()) {
            return ResponseCode::RecordNotFound;
	  } else if (!status.ok()) {
            return ResponseCode::Error;
	  }
	}
        leveldb::WriteOptions options;
        options.sync = syncmode ? true : false;
	leveldb::Status status = itr->second->Put(options, key, value);
        if (!status.ok()) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type remove(const std::string& mapName, const std::string& key) {
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        leveldb::WriteOptions options;
        options.sync = false;
        leveldb::Status status = itr->second->Delete(options, key);
        if (status.IsNotFound()) {
            return ResponseCode::RecordNotFound;
        } else if (!status.ok()) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

private:
    std::string directoryName_; // directory to store db files.
    uint32_t writeBufferSizeMb_; 
    uint32_t blockCacheSizeMb_; 
    leveldb::Cache* cache_;
    boost::ptr_map<std::string, leveldb::DB> maps_;
    boost::shared_mutex mutex_; // protect map_
};

int main(int argc, char **argv) {
    int port;
    int writeBufferSizeMb;
    int blockCacheSizeMb;
    std::string dir;
    po::variables_map vm;
    po::options_description config("");
    config.add_options()
        ("help,h", "produce help message")
        ("sync,s", "synchronous writes")
        ("blindinsert,i", "skip record existence check for inserts")
        ("blindupdate,u",  "skip record existence check for updates")
        ("port,p", po::value<int>(&port)->default_value(9090), "port to listen to")
        ("datadir,d", po::value<std::string>(&dir)->default_value("data"), "data directory")
        ("write-buffer-mb,w", po::value<int>(&writeBufferSizeMb)->default_value(1024), "LevelDB write buffer size in MB")
        ("block-cache-mb,b", po::value<int>(&blockCacheSizeMb)->default_value(1024), "LevelDB block cache size in MB")
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
    blindinsert = vm.count("blindinsert");
    blindupdate = vm.count("blindupdate");
    shared_ptr<LevelDbServer> handler(new LevelDbServer(dir, writeBufferSizeMb, blockCacheSizeMb));
    shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    TThreadedServer server (processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    return 0;
}
