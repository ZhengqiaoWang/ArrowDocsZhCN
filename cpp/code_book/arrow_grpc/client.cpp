#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <arrow/filesystem/api.h>

#include <iostream>
#include <string>

#include "common.h"
#include <grpc++/grpc++.h>
#include "arrow_grpc.service.grpc.pb.h"

using namespace std;

void requestDoGet(const std::unique_ptr<::FlightService::Stub> &service)
{
    grpc::ClientContext ctx;

    ::Ticket ticket;
    auto reader = service->DoGet(&ctx, ticket);
    FlightData data;
    bool read_flag = reader->Read(&data);
    while(read_flag)
    {
        read_flag = reader->Read(&data);
    }
    auto status = reader->Finish();
    if (!status.ok())
    {
        printf("request failed: %s\n", status.error_message().c_str());
        return;
    }
    printf("request done\n");
}

int main(int argc, char const *argv[])
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    std::string server_addr(std::string("0.0.0.0:") + std::to_string(SERVER_PORT));
    grpc::ChannelArguments chan_args;
    chan_args.SetMaxReceiveMessageSize(INT_MAX);

    auto channel = grpc::CreateCustomChannel(server_addr, grpc::InsecureChannelCredentials(), chan_args);
    auto service = FlightService::NewStub(channel);
    requestDoGet(service);
    return 0;
}
