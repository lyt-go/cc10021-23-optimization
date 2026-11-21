"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import delivery, resource

# 投递恢复路由含固定前缀 /api/delivery，需在动态 /api/{module} 之前注册，
# 否则固定路径会被资源路由抢先匹配。
ROUTERS = [delivery, resource]
