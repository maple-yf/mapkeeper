## Requirements:

### Boost
  
    wget http://superb-sea2.dl.sourceforge.net/project/boost/boost/1.48.0/boost_1_48_0.tar.gz
    tar xfvz boost_1_48_0.tar.gz
    cd boost_1_48_0
    ./bootstrap.sh --prefix=/usr/local
    sudo ./b2 install 

### LevelDB

    svn checkout http://leveldb.googlecode.com/svn/trunk/ leveldb-read-only
    cd leveldb-read-only/
    make
    sudo cp libleveldb.a /usr/local/lib
    sudo cp -r include/leveldb /usr/local/include
