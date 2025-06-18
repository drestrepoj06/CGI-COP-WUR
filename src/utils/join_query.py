import asyncio
import redis

# Tile38 setup
TILE38_HOST = 'tile38'
TILE38_PORT = 9851

async def record_ambulance_path(route_points, timestamp, route_estimated_time, ambulance_id):
    """
    连接 Tile38 数据库，并记录救护车路径。
    """    
    tile38_client = redis.Redis(host=TILE38_HOST, port=TILE38_PORT, decode_responses=True)

    # 遍历所有路径点，并计算时间偏移量
    for index, (lat, lng) in enumerate(route_points):
        adjusted_timestamp = timestamp + (index * route_estimated_time // len(route_points) * 50)

        # 执行 Tile38 SET 命令
        command = f"SET ambu_path {ambulance_id}_{adjusted_timestamp} POINT {lat} {lng} {adjusted_timestamp}"

        # command = f"SET ambu_path {ambulance_id} POINT {lat} {lng} {adjusted_timestamp}"
        tile38_client.execute_command(command)

        print(f"Recorded: {command}")

    print("所有救护车路径点已存入 Tile38.")

