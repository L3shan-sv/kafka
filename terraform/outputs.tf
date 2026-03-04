utput "eks_cluster_name" {
  value = aws_eks_cluster.main.name
}

output "kafka_bootstrap_brokers" {
  value     = aws_msk_cluster.kafka.bootstrap_brokers
  sensitive = true
}

output "redis_endpoint" {
  value = aws_elasticache_cluster.redis.cache_nodes[0].address
}

output "alb_dns_name" {
  value = aws_lb.main.dns_name
  description = "Point your domain DNS here"
}