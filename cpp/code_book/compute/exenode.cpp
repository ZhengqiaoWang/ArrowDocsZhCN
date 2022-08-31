
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/dataset/dataset.h>
#include <arrow/compute/api.h>
#include <arrow/compute/registry.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/api_vector.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/util/vector.h>

#include <iostream>
using namespace std;

#include "common.h"

/**
 * @brief 执行计划，生成Table
 *
 * @param exec_context
 * @param plan
 * @param schema
 * @param sink_gen
 * @return arrow::Status
 */
arrow::Status ExecutePlanAndCollectAsTable(
    arrow::compute::ExecContext &exec_context, std::shared_ptr<arrow::compute::ExecPlan> plan,
    std::shared_ptr<arrow::Schema> schema,
    arrow::AsyncGenerator<arrow::util::optional<arrow::compute::ExecBatch>> sink_gen)
{
    // 将sink_gen从异步转换成同步的sink_reader
    std::shared_ptr<arrow::RecordBatchReader> sink_reader =
        arrow::compute::MakeGeneratorReader(schema, std::move(sink_gen), exec_context.memory_pool());

    // 验证plan
    ARROW_RETURN_NOT_OK(plan->Validate());
    std::cout << "ExecPlan created : " << plan->ToString() << std::endl;
    // 开始执行plan
    ARROW_RETURN_NOT_OK(plan->StartProducing());

    // 将sink_reader的数据导入到Table结构中
    std::shared_ptr<arrow::Table> response_table;

    ARROW_ASSIGN_OR_RAISE(response_table,
                          arrow::Table::FromRecordBatchReader(sink_reader.get()));

    std::cout << "Results : " << response_table->ToString() << std::endl;

    // 停止plan
    plan->StopProducing();
    // 标记plan结束
    auto future = plan->finished();
    return future.status();
}

arrow::Status opers()
{
    ARROW_ASSIGN_OR_RAISE(auto table, CreateTable());
    ARROW_ASSIGN_OR_RAISE(auto rb, table->CombineChunksToBatch(arrow::default_memory_pool()));
    cout << rb->schema()->ToString() << endl;
    std::vector<arrow::compute::ExecBatch> batches;
    for (int i = 0; i < 1; ++i)
    {
        ARROW_ASSIGN_OR_RAISE(auto res_batch, rb->SelectColumns({0, 1, 2}));
        batches.emplace_back(*res_batch);
    }

    arrow::compute::ExecContext exec_context;
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::compute::ExecPlan> plan, arrow::compute::ExecPlan::Make(&exec_context));

    arrow::AsyncGenerator<arrow::util::optional<arrow::compute::ExecBatch>> sink_gen;
    arrow::AsyncGenerator<arrow::util::optional<arrow::compute::ExecBatch>> table_gen; // 用于读表的生成器（异步的）
    auto opt_batches = arrow::internal::MapVector(
        [](arrow::compute::ExecBatch batch)
        { return arrow::util::make_optional(std::move(batch)); },
        batches);
    table_gen = arrow::MakeVectorGenerator(std::move(opt_batches));

    // 第一步：加载数据
    auto source_node_options = arrow::compute::SourceNodeOptions{table->schema(), table_gen};
    ARROW_ASSIGN_OR_RAISE(arrow::compute::ExecNode * source, arrow::compute::MakeExecNode("source", plan.get(), {}, source_node_options));

    // 第二步：统计数据：统计非空数据
    auto options = std::make_shared<arrow::compute::ScalarAggregateOptions>(true);
    arrow::compute::Aggregate arr;

    auto aggregate_options =
        arrow::compute::AggregateNodeOptions{/*aggregates=*/{{"hash_mean", options, "a", "mean(a)"}}, // 计算函数, 函数参数, 用于计算的列, 结果列
                                             /*keys=*/{"c"}};
    ARROW_ASSIGN_OR_RAISE(
        arrow::compute::ExecNode * aggregate,
        arrow::compute::MakeExecNode("aggregate", plan.get(), {source}, aggregate_options));

    // 第三步：设置读取用的生成器
    ARROW_RETURN_NOT_OK(
        arrow::compute::MakeExecNode("sink", plan.get(), {aggregate}, arrow::compute::SinkNodeOptions{&sink_gen}));

    // 设置输出结构
    auto schema = arrow::schema({
        arrow::field("mean(a)", arrow::int64()),
        arrow::field("c", arrow::int64()),
    });

    // 执行plan，形成结果表
    return ExecutePlanAndCollectAsTable(exec_context, plan, schema, sink_gen);
}

int main(int argc, char const *argv[])
{
    cout << opers() << endl;
    return 0;
}
