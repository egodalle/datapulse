/**
 * DataPulse API Client
 * Copy this file to your Lovable UI project
 * 
 * Usage:
 *   import { datapulseAPI } from './api';
 *   const dashboard = await datapulseAPI.getDashboard();
 */

import type {
  Platform,
  DashboardSummary,
  PlatformOverview,
  DailySnapshot,
  RevenueSummary,
  ProductPerformance,
  StoreConnection,
  HealthStatus,
  SchemaHealth,
} from './types';

// ⚠️ IMPORTANT: Replace this with your actual API URL
// For local development: 'http://localhost:6000'
// For ngrok tunnel: 'https://your-tunnel.ngrok-free.app'
// For production: 'https://your-deployed-api.com'
const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:6000';

/**
 * Base fetch wrapper with error handling
 */
async function fetchAPI<T>(endpoint: string, options?: RequestInit): Promise<T> {
  const url = `${API_BASE_URL}${endpoint}`;
  
  try {
    const response = await fetch(url, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...options?.headers,
      },
    });

    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Unknown error' }));
      throw new Error(error.detail || `HTTP ${response.status}`);
    }

    return response.json();
  } catch (error) {
    if (error instanceof Error) {
      throw error;
    }
    throw new Error('Network error - please check your connection');
  }
}

/**
 * DataPulse API Client
 */
export const datapulseAPI = {
  // ============== Health Endpoints ==============
  
  /**
   * Check API health
   */
  async health(): Promise<HealthStatus> {
    return fetchAPI('/health');
  },

  /**
   * Check database connection
   */
  async healthDB(): Promise<HealthStatus> {
    return fetchAPI('/health/db');
  },

  /**
   * Check schemas and tables
   */
  async healthSchemas(): Promise<SchemaHealth> {
    return fetchAPI('/health/schemas');
  },

  // ============== KPI Endpoints ==============

  /**
   * Get main dashboard summary with all key KPIs
   */
  async getDashboard(): Promise<DashboardSummary> {
    return fetchAPI('/api/v1/kpis/dashboard');
  },

  /**
   * Get overview metrics for all connected platforms
   */
  async getPlatforms(): Promise<PlatformOverview[]> {
    return fetchAPI('/api/v1/kpis/platforms');
  },

  /**
   * Get daily KPI snapshots
   * @param options.startDate - Start date (YYYY-MM-DD)
   * @param options.endDate - End date (YYYY-MM-DD)
   * @param options.limit - Number of days to return (default: 30)
   */
  async getDailySnapshots(options?: {
    startDate?: string;
    endDate?: string;
    limit?: number;
  }): Promise<DailySnapshot[]> {
    const params = new URLSearchParams();
    if (options?.startDate) params.append('start_date', options.startDate);
    if (options?.endDate) params.append('end_date', options.endDate);
    if (options?.limit) params.append('limit', options.limit.toString());
    
    const query = params.toString() ? `?${params}` : '';
    return fetchAPI(`/api/v1/kpis/daily${query}`);
  },

  /**
   * Get revenue metrics by platform
   * @param options.platform - Filter by platform (optional)
   * @param options.startDate - Start date (YYYY-MM-DD)
   * @param options.endDate - End date (YYYY-MM-DD)
   */
  async getRevenue(options?: {
    platform?: Platform;
    startDate?: string;
    endDate?: string;
  }): Promise<RevenueSummary[]> {
    const params = new URLSearchParams();
    if (options?.platform) params.append('platform', options.platform);
    if (options?.startDate) params.append('start_date', options.startDate);
    if (options?.endDate) params.append('end_date', options.endDate);
    
    const query = params.toString() ? `?${params}` : '';
    return fetchAPI(`/api/v1/kpis/revenue${query}`);
  },

  /**
   * Get product performance metrics
   * @param options.platform - Filter by platform (optional)
   * @param options.tier - Filter by performance tier (optional)
   * @param options.limit - Number of products to return (default: 50)
   */
  async getProducts(options?: {
    platform?: Platform;
    tier?: string;
    limit?: number;
  }): Promise<ProductPerformance[]> {
    const params = new URLSearchParams();
    if (options?.platform) params.append('platform', options.platform);
    if (options?.tier) params.append('tier', options.tier);
    if (options?.limit) params.append('limit', options.limit.toString());
    
    const query = params.toString() ? `?${params}` : '';
    return fetchAPI(`/api/v1/kpis/products${query}`);
  },

  /**
   * Get today's summary metrics
   */
  async getTodaySummary(): Promise<DailySnapshot | { message: string }> {
    return fetchAPI('/api/v1/kpis/summary/today');
  },

  // ============== Store Endpoints ==============

  /**
   * List all store connections
   */
  async getStores(): Promise<StoreConnection[]> {
    return fetchAPI('/api/v1/stores/');
  },

  /**
   * Get specific store connection status
   */
  async getStore(platform: Platform): Promise<StoreConnection> {
    return fetchAPI(`/api/v1/stores/${platform}`);
  },

  /**
   * Connect a new store
   */
  async connectStore(platform: Platform, credentials: Record<string, string>): Promise<StoreConnection> {
    return fetchAPI('/api/v1/stores/connect', {
      method: 'POST',
      body: JSON.stringify({ platform, credentials }),
    });
  },

  /**
   * Disconnect a store
   */
  async disconnectStore(platform: Platform): Promise<{ message: string }> {
    return fetchAPI(`/api/v1/stores/${platform}`, {
      method: 'DELETE',
    });
  },

  /**
   * Trigger a manual sync for a platform
   */
  async triggerSync(platform: Platform): Promise<{ message: string; status: string; job_id: string }> {
    return fetchAPI(`/api/v1/stores/${platform}/sync`, {
      method: 'POST',
    });
  },
};

// Export types for convenience
export type {
  Platform,
  DashboardSummary,
  PlatformOverview,
  DailySnapshot,
  RevenueSummary,
  ProductPerformance,
  StoreConnection,
  HealthStatus,
  SchemaHealth,
};

// Default export
export default datapulseAPI;

