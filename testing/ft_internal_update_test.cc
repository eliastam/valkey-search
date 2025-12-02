/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/commands/commands.h"
#include "src/coordinator/metadata_manager.h"
#include "src/index_schema.pb.h"
#include "testing/common.h"
#include "vmsdk/src/module.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

using ::testing::HasSubstr;

class FTInternalUpdateTest : public ValkeySearchTest {
 protected:
  void SetUp() override {
    ValkeySearchTest::SetUp();
  }
  
  // Helper to create ValkeyModuleString array from string vector
  std::vector<ValkeyModuleString*> CreateArgv(const std::vector<std::string>& args) {
    std::vector<ValkeyModuleString*> argv;
    for (const auto& arg : args) {
      argv.push_back(TestValkeyModule_CreateStringPrintf(&fake_ctx_, "%s", arg.c_str()));
    }
    return argv;
  }
};

TEST_F(FTInternalUpdateTest, DISABLED_InvalidArgumentCount) {
  // Test with no arguments (just command name)
  std::vector<std::string> args = {"FT.INTERNAL_UPDATE"};
  auto argv = CreateArgv(args);
  
  auto result = FTInternalUpdateCmd(&fake_ctx_, argv.data(), argv.size());
  EXPECT_EQ(result.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(result.message(), HasSubstr("wrong number of arguments"));

  // Test with too few arguments
  args = {"FT.INTERNAL_UPDATE", "CREATE"};
  argv = CreateArgv(args);
  result = FTInternalUpdateCmd(&fake_ctx_, argv.data(), argv.size());
  EXPECT_EQ(result.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(result.message(), HasSubstr("wrong number of arguments"));

  // Test with too many arguments
  args = {"FT.INTERNAL_UPDATE", "CREATE", "data", "header", "extra"};
  argv = CreateArgv(args);
  result = FTInternalUpdateCmd(&fake_ctx_, argv.data(), argv.size());
  EXPECT_EQ(result.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(result.message(), HasSubstr("wrong number of arguments"));
}

TEST_F(FTInternalUpdateTest, DISABLED_InvalidMetadataFormat) {
  // Test with invalid metadata that cannot be parsed
  std::vector<std::string> args = {
      "FT.INTERNAL_UPDATE", "CREATE", "invalid_metadata", "invalid_header"};
  auto argv = CreateArgv(args);
  
  auto result = FTInternalUpdateCmd(&fake_ctx_, argv.data(), argv.size());
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.message(), HasSubstr("failed to parse metadata entry"));
}

TEST_F(FTInternalUpdateTest, DISABLED_InvalidHeaderFormat) {
  // Create valid metadata but invalid header
  coordinator::GlobalMetadataEntry entry;
  entry.set_version(1);
  entry.set_encoding_version(1);
  entry.set_fingerprint(12345);
  
  std::string metadata_binary;
  entry.SerializeToString(&metadata_binary);
  
  std::vector<std::string> args = {
      "FT.INTERNAL_UPDATE", "CREATE", metadata_binary, "invalid_header"};
  auto argv = CreateArgv(args);
  
  auto result = FTInternalUpdateCmd(&fake_ctx_, argv.data(), argv.size());
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.message(), HasSubstr("failed to parse"));
}

TEST_F(FTInternalUpdateTest, DISABLED_EmptyMetadata) {
  // Test with empty metadata
  coordinator::GlobalMetadataVersionHeader header;
  header.set_top_level_version(1);
  header.set_top_level_fingerprint(67890);
  
  std::string header_binary;
  header.SerializeToString(&header_binary);
  
  std::vector<std::string> args = {
      "FT.INTERNAL_UPDATE", "CREATE", "", header_binary};
  auto argv = CreateArgv(args);
  
  auto result = FTInternalUpdateCmd(&fake_ctx_, argv.data(), argv.size());
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.message(), HasSubstr("failed to parse metadata entry"));
}

TEST_F(FTInternalUpdateTest, DISABLED_EmptyHeader) {
  // Create valid metadata but empty header
  coordinator::GlobalMetadataEntry entry;
  entry.set_version(1);
  entry.set_encoding_version(1);
  entry.set_fingerprint(12345);
  
  std::string metadata_binary;
  entry.SerializeToString(&metadata_binary);
  
  std::vector<std::string> args = {
      "FT.INTERNAL_UPDATE", "CREATE", metadata_binary, ""};
  auto argv = CreateArgv(args);
  
  auto result = FTInternalUpdateCmd(&fake_ctx_, argv.data(), argv.size());
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.message(), HasSubstr("failed to parse"));
}

TEST_F(FTInternalUpdateTest, DISABLED_CommandPermissions) {
  // Verify that FT.INTERNAL_UPDATE has correct ACL permissions
  // This test checks the permission constants defined in commands.h
  const auto& permissions = kInternalUpdateCmdPermissions;
  
  // Should only have @admin category, no @fast
  EXPECT_EQ(permissions.size(), 1);
  EXPECT_TRUE(permissions.contains(kAdminCategory));
  EXPECT_FALSE(permissions.contains(kFastCategory));
  EXPECT_FALSE(permissions.contains(kReadCategory));
  EXPECT_FALSE(permissions.contains(kWriteCategory));
  EXPECT_FALSE(permissions.contains(kSearchCategory));
}

}  // namespace

}  // namespace valkey_search
