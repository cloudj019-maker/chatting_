#!/bin/bash
set -e

# 1. Redis 6개 노드 실행
echo "📦 Docker Compose로 Redis 노드 6개 실행..."
docker-compose up -d

# 2. 모든 컨테이너가 뜰 때까지 대기
echo "⏳ Redis 컨테이너가 준비될 때까지 5초 대기..."
sleep 5

# 3. 클러스터 생성 (3 master + 3 replica)
echo "🔗 Redis Cluster 생성..."
docker exec -it redis-node1 redis-cli --cluster create \
  redis-node1:6379 redis-node2:6379 redis-node3:6379 \
  redis-node4:6379 redis-node5:6379 redis-node6:6379 \
  --cluster-replicas 1 <<EOF
yes
EOF

# 4. 클러스터 상태 확인
echo "✅ 클러스터 상태 확인..."
docker exec -it redis-node1 redis-cli cluster nodes
