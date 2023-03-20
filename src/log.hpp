// (C) 2023 Martin Huenniger

#pragma once

#include "types.hpp"

#include <memory>
#include <vector>

namespace consensus
{

class MessageBase;

struct LogEntry
{
    LogEntry(LogEntry const & other);

    LogEntry(TermType term_, MessageBase const & msg_);

    TermType term {0};
    std::unique_ptr<MessageBase> message {};
};

using LogType = std::vector<LogEntry>;

}