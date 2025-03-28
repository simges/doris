// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/schema_scanner/schema_backend_compaction_tasks.h"

#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
#include "common/compile_check_begin.h"

std::vector<SchemaScanner::ColumnDesc> SchemaBackendCompactionTasksScanner::_s_tbls_columns = {
        //   name,       type,          size
        {"BE_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"FE_HOST", TYPE_VARCHAR, sizeof(StringRef), false},
        {"BASE_COMPACTION_CONCURRENCY", TYPE_BIGINT, sizeof(int64_t), false},
        {"CUMULATIVE_COMPACTION_CONCURRENCY", TYPE_VARCHAR, sizeof(StringRef), false},
        {"MANUAL_COMPACTION_CONCURRENCY", TYPE_BIGINT, sizeof(int64_t), false},
        {"LATEST_COMPACTION_SCORE", TYPE_BIGINT, sizeof(int64_t), false},
        {"CANDIDATE_MAX_SCORE", TYPE_BIGINT, sizeof(int64_t), false},
        {"MANUAL_COMPACTION_CANDIDATES_NUM", TYPE_BIGINT, sizeof(int64_t), false},
};

SchemaBackendCompactionTasksScanner::SchemaBackendCompactionTasksScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_BACKEND_ACTIVE_TASKS) {}

SchemaBackendCompactionTasksScanner::~SchemaBackendCompactionTasksScanner() {}

Status SchemaBackendCompactionTasksScanner::start(RuntimeState* state) {
    _block_rows_limit = state->batch_size();
    return Status::OK();
}

Status SchemaBackendCompactionTasksScanner::get_next_block_internal(vectorized::Block* block,
                                                                bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_block == nullptr) {
        _block = vectorized::Block::create_unique();

        for (int i = 0; i < _s_tbls_columns.size(); ++i) {
            TypeDescriptor descriptor(_s_tbls_columns[i].type);
            auto data_type =
                    vectorized::DataTypeFactory::instance().create_data_type(descriptor, true);
            _block->insert(vectorized::ColumnWithTypeAndName(
                    data_type->create_column(), data_type, _s_tbls_columns[i].name));
        }

        _block->reserve(4096);

    }
    return Status::OK();
}

} // namespace doris