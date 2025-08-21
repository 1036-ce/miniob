#pragma once

#include "common/sys/rc.h"
#include "session/session.h"
#include "sql/operator/physical_operator.h"

class SQLStageEvent;

/**
 * @brief 创建视图的执行器
 * @ingroup Executor
 */
class CreateViewExecutor
{
public:
  CreateViewExecutor()          = default;
  virtual ~CreateViewExecutor() = default;

  RC execute(SQLStageEvent *sql_event);
private:
};
