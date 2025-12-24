/**
 * DataPulse API Type Definitions
 * Copy these types to your Lovable UI project
 */

// Platform types
export type Platform = 'shopify' | 'amazon' | 'lazada' | 'shopee';

// Platform Overview
export interface PlatformOverview {
  platform: Platform;
  total_orders: number;
  completed_orders: number;
  cancelled_orders: number;
  total_revenue_usd: string;
  orders_this_month: number;
  revenue_this_month_usd: string;
  orders_last_month: number;
  revenue_last_month_usd: string;
  orders_today: number;
  revenue_today_usd: string;
  avg_order_value_usd: string;
  avg_items_per_order: string;
  payment_rate: string;
  fulfillment_rate: string | null;
  cancellation_rate: string;
  first_order_date: string;
  last_order_date: string;
  active_days: number;
  revenue_mom_growth_pct: string;
  orders_mom_growth_pct: string;
  _generated_at: string;
}

// Daily Snapshot
export interface DailySnapshot {
  order_date: string;
  total_orders: number;
  total_revenue_usd: string;
  avg_order_value_usd: string;
  total_items_sold: number;
  shopify_orders: number;
  amazon_orders: number;
  lazada_orders: number;
  shopee_orders: number;
  shopify_revenue_usd: string;
  amazon_revenue_usd: string;
  lazada_revenue_usd: string;
  shopee_revenue_usd: string;
  unique_customers: number;
  fulfilled_orders: number;
  fulfillment_rate: string;
  revenue_7d_avg: string | null;
  orders_7d_avg: string | null;
  revenue_30d_avg: string | null;
  orders_30d_avg: string | null;
  revenue_dod_change: string | null;
  orders_dod_change: number | null;
  revenue_wow_change: string | null;
  orders_wow_change: number | null;
  _generated_at: string;
}

// Revenue Summary
export interface RevenueSummary {
  order_date: string;
  platform: Platform;
  total_orders: number;
  gross_revenue: string;
  gross_revenue_usd: string;
  net_revenue: string;
  avg_order_value: string;
  avg_order_value_usd: string;
  paid_orders: number;
  unpaid_orders: number;
  fulfilled_orders: number;
  pending_fulfillment: number;
  prev_day_revenue: string | null;
  revenue_change: string | null;
  mtd_revenue_usd: string;
  mtd_orders: number;
  revenue_growth_pct: string;
  _generated_at: string;
}

// Product Performance
export interface ProductPerformance {
  platform: Platform;
  product_id: string;
  product_name: string;
  sku: string | null;
  total_orders: number;
  total_units_sold: number;
  total_revenue: string;
  avg_selling_price: string;
  days_with_sales: number;
  first_sale_date: string;
  last_sale_date: string;
  units_this_month: number;
  revenue_this_month: string;
  avg_daily_units: string;
  revenue_rank: number;
  units_rank: number;
  revenue_percentile: number;
  performance_tier: 'Top 10' | 'Top Performer' | 'Average' | 'Underperformer';
  _generated_at: string;
}

// Dashboard Summary
export interface DashboardSummary {
  total_revenue_usd: string;
  total_orders: number;
  avg_order_value_usd: string;
  revenue_growth_pct: number;
  orders_growth_pct: number;
  total_customers: number;
  platforms: PlatformOverview[];
  recent_days: DailySnapshot[];
}

// Store Connection
export interface StoreConnection {
  platform: Platform;
  is_connected: boolean;
  last_sync: string | null;
  status: 'connected' | 'not_connected' | 'disconnected' | 'syncing' | 'error';
  connection_id: string | null;
}

// API Response types
export interface ApiError {
  detail: string;
}

// Health check
export interface HealthStatus {
  status: 'healthy' | 'unhealthy';
  database?: 'connected' | 'disconnected';
  error?: string;
}

export interface SchemaHealth {
  status: 'healthy' | 'unhealthy';
  schemas: string[];
  mart_tables: string[];
  error?: string;
}

