/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/commands.h"
#include "src/commands/ft_create_parser.h"
#include "src/index_schema.pb.h"
#include "src/schema_manager.h"

namespace valkey_search {

absl::Status FTInternalUpdateCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
  VMSDK_LOG(WARNING, ctx) << "FT.INTERNAL_UPDATE called with argc=" << argc;
  
  if (argc != 4) {
    return absl::InvalidArgumentError("ERR wrong number of arguments for FT_INTERNAL_UPDATE");
  }
  
  // Parse id
  size_t id_len;
  const char* id_ptr = ValkeyModule_StringPtrLen(argv[1], &id_len);
  std::string id(id_ptr, id_len);
  VMSDK_LOG(WARNING, ctx) << "ID: " << id;
  
  // Deserialize GlobalMetadataEntry
  size_t metadata_len;
  const char* metadata_data = ValkeyModule_StringPtrLen(argv[2], &metadata_len);
  coordinator::GlobalMetadataEntry metadata_entry;
  if (!metadata_entry.ParseFromArray(metadata_data, metadata_len)) {
    return absl::InvalidArgumentError("ERR failed to parse metadata entry");
  }
  
  // Deserialize GlobalMetadataVersionHeader
  size_t header_len;
  const char* header_data = ValkeyModule_StringPtrLen(argv[3], &header_len);
  coordinator::GlobalMetadataVersionHeader version_header;
  if (!version_header.ParseFromArray(header_data, header_len)) {
    return absl::InvalidArgumentError("ERR failed to parse version header");
  }
  
  bool is_deletion = !metadata_entry.has_content() || metadata_entry.content().type_url().empty();
  std::string action = is_deletion ? "DROP" : "CREATE";
  VMSDK_LOG(WARNING, ctx) << "Deserialized objects successfully for " << action << " of " << id;
  
  auto status = coordinator::MetadataManager::Instance().ProcessInternalUpdate(
      ctx, kSchemaManagerMetadataTypeName, id, &metadata_entry, &version_header);
  if (!status.ok()) {
    return status;
  }

  // Check context flags before replication
  int ctx_flags = ValkeyModule_GetContextFlags(ctx);
  VMSDK_LOG(WARNING, ctx) << "Context flags: " << ctx_flags;
  VMSDK_LOG(WARNING, ctx) << "Binary data lengths - metadata: " << metadata_len << ", header: " << header_len;
  
  // Replicate to AOF verbatim for persistence
  VMSDK_LOG(WARNING, ctx) << "About to call ValkeyModule_ReplicateVerbatim for " << action;
  int repl_result = ValkeyModule_ReplicateVerbatim(ctx);
  VMSDK_LOG(WARNING, ctx) << "ValkeyModule_ReplicateVerbatim returned: " << repl_result;

  ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  return absl::OkStatus();
}

}  // namespace valkey_search
