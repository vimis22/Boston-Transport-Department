import {
  useMutation,
  useQuery,
  useQueryClient,
  UseQueryOptions,
} from "@tanstack/react-query";
import { apiFetch } from "./client";
import {
  ClockState,
  DashboardConfig,
  HiveQueryRequest,
  HiveQueryResult,
  KafkaSampleResponse,
  KafkaTopicMetrics,
  KafkaTopicsResponse,
} from "./types";

export const useDashboardConfig = () =>
  useQuery({
    queryKey: ["config"],
    queryFn: () => apiFetch<DashboardConfig>("/api/config"),
  });

export const useClockState = () =>
  useQuery({
    queryKey: ["clock"],
    queryFn: () => apiFetch<ClockState>("/api/timemanager/clock/state"),
    refetchInterval: 1000,
  });

export const useSetClockSpeed = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (speed: number) =>
      apiFetch("/api/timemanager/clock/speed", {
        method: "PUT",
        body: JSON.stringify({ speed }),
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["clock"] }),
  });
};

export const useSetClockTime = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (time: string) =>
      apiFetch("/api/timemanager/clock/time", {
        method: "PUT",
        body: JSON.stringify({ time }),
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["clock"] }),
  });
};

const HIVE_QUERY_KEY = ["hive", "interactive"];

export const useHiveQuery = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (payload: HiveQueryRequest) =>
      apiFetch<HiveQueryResult>("/api/hive/query", {
        method: "POST",
        body: JSON.stringify(payload),
      }),
    onSuccess: (data) => {
      // Store results in query cache so HiveResults can access them
      qc.setQueryData(HIVE_QUERY_KEY, data);
    },
  });
};

export const useHiveQueryResults = () =>
  useQuery<HiveQueryResult | null>({
    queryKey: HIVE_QUERY_KEY,
    queryFn: () => null,
    staleTime: Infinity,
    gcTime: Infinity,
    initialData: null,
  });

export const useKafkaTopics = (options?: UseQueryOptions<KafkaTopicsResponse>) =>
  useQuery({
    queryKey: ["kafka", "topics"],
    queryFn: () => apiFetch<KafkaTopicsResponse>("/api/kafka/topics"),
    refetchInterval: 5000,
    ...(options ?? {}),
  });

export const useKafkaTopicMetrics = (topic?: string) =>
  useQuery({
    queryKey: ["kafka", "metrics", topic],
    enabled: Boolean(topic),
    queryFn: () =>
      apiFetch<KafkaTopicMetrics>(`/api/kafka/topics/${topic}/metrics`),
    refetchInterval: 5000,
  });

export const useKafkaTopicSample = (topic?: string, limit = 20) =>
  useQuery({
    queryKey: ["kafka", "sample", topic, limit],
    enabled: Boolean(topic),
    queryFn: () =>
      apiFetch<KafkaSampleResponse>(
        `/api/kafka/topics/${topic}/sample?limit=${limit}`
      ),
    refetchInterval: 1000,
  });

export const useHistoricalHiveQuery = (
  statement: string,
  enabled = true
) =>
  useQuery({
    queryKey: ["hive", "historical", statement],
    enabled,
    queryFn: () =>
      apiFetch<HiveQueryResult>("/api/hive/query", {
        method: "POST",
        body: JSON.stringify({ statement }),
      }),
    staleTime: 60_000,
  });

// Hook for aggregating Kafka messages over time
// Keeps a rolling window of messages and aggregates them by time bucket
import { useRef, useCallback, useEffect, useState } from "react";

type AggregatedBucket<T> = {
  timestamp: number;
  count: number;
  data: T[];
};

type UseLiveKafkaAggregationOptions<T, A> = {
  topic?: string;
  bucketSizeMs?: number;
  maxBuckets?: number;
  aggregate?: (records: T[]) => A;
};

export function useLiveKafkaAggregation<T = unknown, A = { count: number }>(
  options: UseLiveKafkaAggregationOptions<T, A>
) {
  const { topic, bucketSizeMs = 5000, maxBuckets = 30, aggregate } = options;
  const sample = useKafkaTopicSample(topic, 100);

  // Track seen offsets per partition to avoid double counting
  const seenOffsetsRef = useRef<Map<number, Set<number>>>(new Map());
  const [buckets, setBuckets] = useState<AggregatedBucket<T>[]>([]);
  const lastBucketTimeRef = useRef<number>(0);

  // Default aggregator just counts
  const defaultAggregate = useCallback((records: T[]) => ({ count: records.length } as unknown as A), []);
  const aggregator = aggregate ?? defaultAggregate;

  // Process new records when sample updates
  useEffect(() => {
    if (!sample.data?.records?.length) return;

    const now = Date.now();
    const currentBucketTime = Math.floor(now / bucketSizeMs) * bucketSizeMs;

    // Find new records we haven't seen
    const newRecords: T[] = [];
    for (const record of sample.data.records) {
      const partitionOffsets = seenOffsetsRef.current.get(record.partition);
      if (!partitionOffsets) {
        seenOffsetsRef.current.set(record.partition, new Set([record.offset]));
        newRecords.push(record.value as T);
      } else if (!partitionOffsets.has(record.offset)) {
        partitionOffsets.add(record.offset);
        newRecords.push(record.value as T);
      }
    }

    if (newRecords.length === 0 && currentBucketTime === lastBucketTimeRef.current) return;

    setBuckets((prev) => {
      const updated = [...prev];
      
      // Find or create current bucket
      let currentBucket = updated.find(b => b.timestamp === currentBucketTime);
      if (!currentBucket) {
        currentBucket = { timestamp: currentBucketTime, count: 0, data: [] };
        updated.push(currentBucket);
        updated.sort((a, b) => a.timestamp - b.timestamp);
      }

      // Add new records to current bucket
      if (newRecords.length > 0) {
        currentBucket.data = [...currentBucket.data, ...newRecords];
        currentBucket.count = currentBucket.data.length;
      }

      // Keep only last N buckets
      while (updated.length > maxBuckets) {
        updated.shift();
      }

      lastBucketTimeRef.current = currentBucketTime;
      return updated;
    });
  }, [sample.data, bucketSizeMs, maxBuckets]);

  // Compute aggregated data for each bucket
  const aggregatedData = buckets.map((bucket) => ({
    timestamp: bucket.timestamp,
    time: new Date(bucket.timestamp).toLocaleTimeString(),
    count: bucket.count,
    ...aggregator(bucket.data),
  }));

  // Reset function to clear all data
  const reset = useCallback(() => {
    seenOffsetsRef.current.clear();
    setBuckets([]);
    lastBucketTimeRef.current = 0;
  }, []);

  // Total count across all buckets
  const totalCount = buckets.reduce((sum, b) => sum + b.count, 0);

  return {
    data: aggregatedData,
    totalCount,
    isLoading: sample.isLoading,
    isError: sample.isError,
    reset,
    latestRecords: sample.data?.records ?? [],
  };
}
