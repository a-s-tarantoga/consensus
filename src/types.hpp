// (C) 2023 Martin Huenniger

#pragma once

#include <functional>
#include <unordered_set>

namespace consensus
{

using TermType    = int64_t;
using IdType      = int64_t;
constexpr IdType NoneId = -1;

enum class Role
{
    Follower,
    Candidate,
    Leader
};

using VoteListType = std::unordered_set<IdType>;
}