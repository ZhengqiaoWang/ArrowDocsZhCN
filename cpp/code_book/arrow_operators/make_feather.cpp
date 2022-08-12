
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/filesystem/api.h>
#include <parquet/arrow/writer.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_ipc.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/scanner.h>
#include <arrow/ipc/writer.h>


#include <iostream>
using namespace std;

#include "common.h"

#define DATASET_NAME "slice"

/**
 * @brief 生成一个由两个数据文件组成的数据集
 *
 * @param filesystem fs
 * @param root_path 根目录
 * @return arrow::Result<std::string> 数据集地址
 */
arrow::Result<std::string> CreateExampleParquetDataset(
    const std::shared_ptr<arrow::fs::FileSystem> &filesystem, const std::string &root_path)
{
    auto base_path = root_path + "/" + DATASET_NAME + "_output/feather_dataset";
    ARROW_RETURN_NOT_OK(filesystem->CreateDir(base_path));         // 创建一个数据文件夹
    ARROW_RETURN_NOT_OK(filesystem->DeleteDirContents(base_path)); // 删除文件夹内历史数据
    ARROW_ASSIGN_OR_RAISE(auto table, CreateTable());              // 创建表

    // 写入两个文件里，每个文件五行
    ARROW_ASSIGN_OR_RAISE(auto output,
                          filesystem->OpenOutputStream(base_path + "/data1.feather"));
    ARROW_ASSIGN_OR_RAISE(auto writer,
                          arrow::ipc::MakeFileWriter(output.get(), table->schema()));
    ARROW_RETURN_NOT_OK(writer->WriteTable(*table->Slice(0, 5)));
    ARROW_RETURN_NOT_OK(writer->Close());
    ARROW_ASSIGN_OR_RAISE(output,
                          filesystem->OpenOutputStream(base_path + "/data2.feather"));
    ARROW_ASSIGN_OR_RAISE(writer,
                          arrow::ipc::MakeFileWriter(output.get(), table->schema()));
    ARROW_RETURN_NOT_OK(writer->WriteTable(*table->Slice(5)));
    ARROW_RETURN_NOT_OK(writer->Close());
    return base_path;
}

arrow::Status func()
{
    std::string base_path;
    std::shared_ptr<arrow::dataset::FileFormat> format;
    cout << "uri:" << uri << endl;
    ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(uri, &root_path));

    format = std::make_shared<arrow::dataset::IpcFileFormat>();
    ARROW_ASSIGN_OR_RAISE(base_path, CreateExampleParquetDataset(fs, root_path));

    cout << "base_path:" << base_path << endl;
    cout << "root_path:" << root_path << endl;

    return arrow::Status::OK();
}

int main(int argc, char const *argv[])
{
    cout << func() << endl;
    return 0;
}
