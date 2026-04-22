declare module "lru-cache" {
  export class LRUCache<K, V> {
    constructor(options?: LRUCacheOptions<K, V>);
    get(key: K): V | undefined;
    set(key: K, value: V, options?: { ttl?: number; start?: number }): this;
    has(key: K): boolean;
    delete(key: K): boolean;
    clear(): void;
    keys(): IterableIterator<K>;
    values(): IterableIterator<V>;
    entries(): IterableIterator<[K, V]>;
    size: number;
    max: number;
  }

  export interface LRUCacheOptions<K, V> {
    max?: number;
    ttl?: number;
    ttlAutopurge?: boolean;
    updateAgeOnGet?: boolean;
    updateAgeOnHas?: boolean;
    allowStaleOnTTLReached?: boolean;
    dispose?: (value: V, key: K, reason: "evict" | "set" | "delete") => void;
    disposeAfter?: (value: V, key: K, reason: "evict" | "set" | "delete") => void;
    maxSize?: number;
    sizeCalculation?: (value: V, key: K) => number;
    fetchContext?: unknown;
  }

  export { LRUCache as default };
}
