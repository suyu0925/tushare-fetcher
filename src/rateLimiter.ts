export class RateLimiter {
  private queue: Array<{
    fn: () => Promise<any>;
    resolve: (value: any) => void;
    reject: (reason: any) => void;
  }> = [];
  private tokens: number;
  private lastRefillTime: number;
  private processing: boolean = false;

  constructor(
    private readonly intervalInSeconds: number,
    private readonly maxCalls: number
  ) {
    this.tokens = maxCalls;
    this.lastRefillTime = Date.now();
  }

  private refillTokens() {
    const now = Date.now();
    const timePassed = (now - this.lastRefillTime) / 1000;
    const tokensToAdd = Math.floor(timePassed * (this.maxCalls / this.intervalInSeconds));
    
    this.tokens = Math.min(this.maxCalls, this.tokens + tokensToAdd);
    this.lastRefillTime = now;
  }

  private async processQueue() {
    if (this.processing) return;
    this.processing = true;

    while (this.queue.length > 0) {
      this.refillTokens();

      if (this.tokens <= 0) {
        const waitTime = (this.intervalInSeconds / this.maxCalls) * 1000;
        await new Promise(resolve => setTimeout(resolve, waitTime));
        continue;
      }

      const task = this.queue[0];
      this.tokens--;

      try {
        const result = await task.fn();
        task.resolve(result);
      } catch (error) {
        task.reject(error);
      }

      this.queue.shift();
    }

    this.processing = false;
  }

  async execute<T>(fn: () => Promise<T>): Promise<T> {
    return new Promise((resolve, reject) => {
      this.queue.push({ fn, resolve, reject });
      this.processQueue();
    });
  }
}
