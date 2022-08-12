
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <arrow/filesystem/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>

#include <arrow/filesystem/hdfs.h>

#include <grpc++/grpc++.h>
#include "arrow_grpc.service.pb.h"
#include "arrow_grpc.service.grpc.pb.h"

#include <iostream>
#include <string>
#include "common.h"

using namespace std;

// #2: 读取整个文件
std::shared_ptr<arrow::Table> read_whole_file(std::string file_name)
{
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(file_name, arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

    return table;
}

class TradeQueryImpl : public FlightService::Service
{
protected:
    virtual ::grpc::Status DoGet(::grpc::ServerContext *context, const ::Ticket *ticket, ::grpc::ServerWriter<::FlightData> *writer)
    {
        printf("get do get\n");
        // FlightData empty;
        // writer->Write(empty);

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        auto table = read_whole_file(PARQUET_FILE_DIR PARQUET_FILE_NAME);
        arrow::TableBatchReader batch_reader(table);

        auto rst_batches = batch_reader.ToRecordBatches();
        if (!rst_batches.ok())
        {
            printf("%s\n", rst_batches.status().message().c_str());
            return ::grpc::Status::OK;
        }
        batches = rst_batches.MoveValueUnsafe();
        auto rst_owning_reader = arrow::RecordBatchReader::Make(
            std::move(batches), table->schema());
        if (!rst_owning_reader.ok())
        {
            printf("%s\n", rst_owning_reader.status().message().c_str());
            return ::grpc::Status::OK;
        }
        auto owning_reader = rst_owning_reader.ValueUnsafe();

        // owning_reader->Next();
        auto rst_tmp_batch = owning_reader->Next();
        if (!rst_tmp_batch.ok())
        {
            printf("%s\n", rst_tmp_batch.status().message().c_str());
            return ::grpc::Status::OK;
        }
        auto tmp_batch = rst_tmp_batch.ValueUnsafe();

        ArrowArray out;
        arrow::ExportRecordBatch(*tmp_batch, &out);

        printf("done do get\n");
        return ::grpc::Status::OK;
    }
};

int main(int argc, char const *argv[])
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    std::string server_addr(std::string("0.0.0.0:") + std::to_string(SERVER_PORT));
    grpc::EnableDefaultHealthCheckService(true);

    TradeQueryImpl service;

    grpc::ServerBuilder builder;
    builder.AddChannelArgument("MaxSendMessageSize", INT_MAX);
    builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    if (nullptr == server)
    {
        return -1;
    }
    server->Wait();

    // auto table = read_whole_file(PARQUET_FILE_DIR PARQUET_FILE_NAME);
    // auto stream =
    return 0;
}
