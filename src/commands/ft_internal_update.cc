/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/commands.h"
#include "src/commands/ft_create_parser.h"
#include "src/coordinator/metadata_manager.h"
#include "src/index_schema.pb.h"
#include "src/metrics.h"
#include "src/schema_manager.h"
#include "src/valkey_search_options.h"

namespace valkey_search {

constexpr int kFTInternalUpdateArgCount = 4;

absl::Status FTInternalUpdateCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
  if (argc != kFTInternalUpdateArgCount) {
    return absl::InvalidArgumentError("ERR wrong number of arguments for FT_INTERNAL_UPDATE");
  }
  
  // Parse index ID
  size_t id_len;
  const char *id_data = ValkeyModule_StringPtrLen(argv[1], &id_len);
  std::string id(id_data, id_len);
  
  // Deserialize GlobalMetadataEntry
  size_t metadata_len;
  const char *metadata_data = ValkeyModule_StringPtrLen(argv[2], &metadata_len);
  coordinator::GlobalMetadataEntry metadata_entry;
  if (!metadata_entry.ParseFromArray(metadata_data, metadata_len)) {
    VMSDK_LOG(WARNING, ctx) << "CRITICAL: Failed to parse GlobalMetadataEntry in FT.INTERNAL_UPDATE. "
                          << "Data length: " << metadata_len << ", Index ID: " << id
                          << ". This indicates data corruption or logic bug.";
    Metrics::GetStats().ft_internal_update_parse_failures_cnt++;
    
    if (ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_LOADING) {
      if (options::GetSkipCorruptedAOFEntries().GetValue()) {
        VMSDK_LOG(WARNING, ctx) << "SKIPPING corrupted AOF entry due to configuration";
        Metrics::GetStats().ft_internal_update_skipped_entries_cnt++;
        ValkeyModule_ReplyWithSimpleString(ctx, "OK");
        return absl::OkStatus();
      } else {
        CHECK(false) << "Protobuf parse failure during AOF loading - cannot continue";
      }
    }
    
    return absl::InvalidArgumentError("ERR failed to parse GlobalMetadataEntry");
  }
  
  // Deserialize GlobalMetadataVersionHeader
  size_t header_len;
  const char *header_data = ValkeyModule_StringPtrLen(argv[3], &header_len);
  coordinator::GlobalMetadataVersionHeader version_header;
  if (!version_header.ParseFromArray(header_data, header_len)) {
    VMSDK_LOG(WARNING, ctx) << "CRITICAL: Failed to parse GlobalMetadataVersionHeader in FT.INTERNAL_UPDATE. "
                          << "Data length: " << header_len << ", Index ID: " << id
                          << ". This indicates data corruption or logic bug.";
    Metrics::GetStats().ft_internal_update_parse_failures_cnt++;
    
    if (ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_LOADING) {
      if (options::GetSkipCorruptedAOFEntries().GetValue()) {
        VMSDK_LOG(WARNING, ctx) << "SKIPPING corrupted AOF entry due to configuration";
        Metrics::GetStats().ft_internal_update_skipped_entries_cnt++;
        ValkeyModule_ReplyWithSimpleString(ctx, "OK");
        return absl::OkStatus();
      } else {
        CHECK(false) << "Protobuf parse failure during AOF loading - cannot continue";
      }
    }
    
    return absl::InvalidArgumentError("ERR failed to parse GlobalMetadataVersionHeader");
  }
  
  auto status = coordinator::MetadataManager::Instance().ProcessInternalUpdate(
      ctx, kSchemaManagerMetadataTypeName, id, &metadata_entry, &version_header);
  if (!status.ok()) {
    VMSDK_LOG(WARNING, ctx) << "CRITICAL: ProcessInternalUpdate failed in FT.INTERNAL_UPDATE. "
                          << "Index ID: " << id << ", Error: " << status.message();

    Metrics::GetStats().ft_internal_update_process_failures_cnt++;
    if (options::GetSkipCorruptedAOFEntries().GetValue()) {
      VMSDK_LOG(WARNING, ctx) << "SKIPPING corrupted AOF entry due to configuration";
      Metrics::GetStats().ft_internal_update_skipped_entries_cnt++;
      ValkeyModule_ReplyWithSimpleString(ctx, "OK");
      return absl::OkStatus();
    } else {
      CHECK(false) << "Protobuf parse failure during AOF loading - cannot continue";
    }

    return status;
  }

  ValkeyModule_ReplicateVerbatim(ctx);

  ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  return absl::OkStatus();
}

}  // namespace valkey_search
