FROM quickwit/cross-base:x86_64-unknown-linux-musl
# See https://github.com/quickwit-inc/rust-musl-builder

RUN echo "Upgrading CMake" && \
    sudo apt-get remove cmake -y && \
    curl -fLO https://www.cmake.org/files/v3.12/cmake-3.12.1.tar.gz && \
    tar -xvzf cmake-3.12.1.tar.gz && \
    cd cmake-3.12.1/ && ./configure && \
    sudo make install


RUN echo "Installing protoc" && \
    curl -fLO "https://github.com/protocolbuffers/protobuf/releases/download/v3.20.1/protoc-3.20.1-linux-x86_64.zip" && \
    unzip protoc-3.20.1-linux-x86_64.zip -d ./protoc/ && \
    sudo cp ./protoc/bin/protoc /usr/bin/protoc

RUN echo "Installing rocksdb dependencies" && \
    sudo apt-get update && \ 
    sudo apt-get install -y libclang-3.9-dev clang-3.9

RUN echo "Build rocks db" && \
    git clone https://github.com/facebook/rocksdb.git && \
    cd rocksdb  && \
    git submodule update --init --recursive  && \
    git checkout 00724f43bcea4d82b371  && \
    make static_lib -j4  && \
    sudo cp ./librocksdb.a /usr/lib/librocksdb.a

#COPY clink.sh /

ENV CC=musl-gcc \
    #CXX=musl-g++ \
    CFLAGS=-I/usr/local/musl/include \
    LIB_LDFLAGS=-L/usr/lib/x86_64-linux-gnu \
    PROTOC=/usr/bin/protoc \
    PROTOC_INCLUDE=/usr/include \
    ROCKSDB_LIB_DIR=/usr/lib \
    ROCKSDB_STATIC=1 \
    #CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=/clink.sh
