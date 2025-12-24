/**
 * DataPulse React Hooks
 * Copy this file to your Lovable UI project
 * 
 * Usage:
 *   import { useDashboard, usePlatforms } from './hooks';
 *   
 *   function Dashboard() {
 *     const { data, loading, error } = useDashboard();
 *     if (loading) return <Spinner />;
 *     if (error) return <Error message={error} />;
 *     return <DashboardView data={data} />;
 *   }
 */

import { useState, useEffect, useCallback } from 'react';
import { datapulseAPI } from './api';
import type {
  Platform,
  DashboardSummary,
  PlatformOverview,
  DailySnapshot,
  RevenueSummary,
  ProductPerformance,
  StoreConnection,
} from './types';

// Generic hook state
interface UseQueryState<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

/**
 * Generic data fetching hook
 */
function useQuery<T>(
  fetchFn: () => Promise<T>,
  dependencies: unknown[] = []
): UseQueryState<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await fetchFn();
      setData(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  }, dependencies);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  return { data, loading, error, refetch: fetchData };
}

// ============== Dashboard Hooks ==============

/**
 * Fetch main dashboard data
 */
export function useDashboard(): UseQueryState<DashboardSummary> {
  return useQuery(() => datapulseAPI.getDashboard(), []);
}

/**
 * Fetch platform overview
 */
export function usePlatforms(): UseQueryState<PlatformOverview[]> {
  return useQuery(() => datapulseAPI.getPlatforms(), []);
}

/**
 * Fetch daily snapshots
 */
export function useDailySnapshots(options?: {
  startDate?: string;
  endDate?: string;
  limit?: number;
}): UseQueryState<DailySnapshot[]> {
  return useQuery(
    () => datapulseAPI.getDailySnapshots(options),
    [options?.startDate, options?.endDate, options?.limit]
  );
}

/**
 * Fetch revenue data
 */
export function useRevenue(options?: {
  platform?: Platform;
  startDate?: string;
  endDate?: string;
}): UseQueryState<RevenueSummary[]> {
  return useQuery(
    () => datapulseAPI.getRevenue(options),
    [options?.platform, options?.startDate, options?.endDate]
  );
}

/**
 * Fetch product performance
 */
export function useProducts(options?: {
  platform?: Platform;
  tier?: string;
  limit?: number;
}): UseQueryState<ProductPerformance[]> {
  return useQuery(
    () => datapulseAPI.getProducts(options),
    [options?.platform, options?.tier, options?.limit]
  );
}

/**
 * Fetch store connections
 */
export function useStores(): UseQueryState<StoreConnection[]> {
  return useQuery(() => datapulseAPI.getStores(), []);
}

// ============== Utility Hooks ==============

/**
 * Auto-refresh hook for real-time data
 */
export function useAutoRefresh<T>(
  fetchFn: () => Promise<T>,
  intervalMs: number = 60000 // Default: 1 minute
): UseQueryState<T> & { pause: () => void; resume: () => void } {
  const [isPaused, setIsPaused] = useState(false);
  const queryState = useQuery(fetchFn, []);

  useEffect(() => {
    if (isPaused) return;

    const interval = setInterval(() => {
      queryState.refetch();
    }, intervalMs);

    return () => clearInterval(interval);
  }, [isPaused, intervalMs, queryState.refetch]);

  return {
    ...queryState,
    pause: () => setIsPaused(true),
    resume: () => setIsPaused(false),
  };
}

/**
 * Dashboard with auto-refresh
 */
export function useLiveDashboard(intervalMs: number = 60000) {
  return useAutoRefresh(() => datapulseAPI.getDashboard(), intervalMs);
}

// ============== Formatted Data Hooks ==============

/**
 * Format currency values
 */
export function useFormattedDashboard() {
  const { data, ...rest } = useDashboard();

  const formatted = data ? {
    ...data,
    total_revenue_formatted: formatCurrency(data.total_revenue_usd),
    avg_order_value_formatted: formatCurrency(data.avg_order_value_usd),
    platforms: data.platforms.map(p => ({
      ...p,
      total_revenue_formatted: formatCurrency(p.total_revenue_usd),
      revenue_this_month_formatted: formatCurrency(p.revenue_this_month_usd),
      revenue_today_formatted: formatCurrency(p.revenue_today_usd),
    })),
  } : null;

  return { data: formatted, ...rest };
}

// ============== Helper Functions ==============

/**
 * Format number as currency
 */
function formatCurrency(value: string | number, currency: string = 'USD'): string {
  const num = typeof value === 'string' ? parseFloat(value) : value;
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency,
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(num);
}

/**
 * Format percentage
 */
export function formatPercent(value: string | number): string {
  const num = typeof value === 'string' ? parseFloat(value) : value;
  return `${num >= 0 ? '+' : ''}${num.toFixed(1)}%`;
}

/**
 * Format large numbers
 */
export function formatNumber(value: number): string {
  if (value >= 1000000) {
    return `${(value / 1000000).toFixed(1)}M`;
  }
  if (value >= 1000) {
    return `${(value / 1000).toFixed(1)}K`;
  }
  return value.toString();
}

/**
 * Get platform display name
 */
export function getPlatformName(platform: Platform): string {
  const names: Record<Platform, string> = {
    shopify: 'Shopify',
    amazon: 'Amazon',
    lazada: 'Lazada',
    shopee: 'Shopee',
  };
  return names[platform] || platform;
}

/**
 * Get platform color
 */
export function getPlatformColor(platform: Platform): string {
  const colors: Record<Platform, string> = {
    shopify: '#96bf48',  // Shopify green
    amazon: '#ff9900',   // Amazon orange
    lazada: '#0f146d',   // Lazada blue
    shopee: '#ee4d2d',   // Shopee orange-red
  };
  return colors[platform] || '#666666';
}

