# Uncomment for Challenge #7
import time
import random
from redis.client import Redis

from redisolar.dao.base import RateLimiterDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema
# Uncomment for Challenge #7
from redisolar.dao.base import RateLimitExceededException


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""
    def __init__(self,
                 window_size_ms: float,
                 max_hits: int,
                 redis_client: Redis,
                 key_schema: KeySchema = None,
                 **kwargs):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        super().__init__(redis_client, key_schema, **kwargs)

    def hit(self, name: str):
        """Record a hit using the rate-limiter."""
        # START Challenge #7
        pipeline = self.redis.pipeline(transaction=True)

        slidind_window_z_key = self.key_schema.sliding_window_rate_limiter_key(
            name, self.window_size_ms, self.max_hits
        )
        now_ms = int(time.time() * 1000)
        rand = random.random()

        pipeline.zadd(slidind_window_z_key, {f'{now_ms}-{rand}': now_ms})
        pipeline.zremrangebyscore(slidind_window_z_key, 0, now_ms - self.window_size_ms)
        pipeline.zcard(slidind_window_z_key)

        *_, hits = pipeline.execute()

        if hits > self.max_hits:
            raise RateLimitExceededException()
        # END Challenge #7
