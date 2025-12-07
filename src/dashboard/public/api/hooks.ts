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

export const useHiveQuery = () =>
  useMutation({
    mutationFn: (payload: HiveQueryRequest) =>
      apiFetch<HiveQueryResult>("/api/hive/query", {
        method: "POST",
        body: JSON.stringify(payload),
      }),
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

