/**
 * DataPulse Dashboard Example Component
 * Copy and adapt this component for your Lovable UI
 */

import React from 'react';
import { 
  useDashboard, 
  useDailySnapshots,
  useProducts,
  formatPercent,
  formatNumber,
  getPlatformName,
  getPlatformColor 
} from '../hooks';
import type { Platform, PlatformOverview } from '../types';

// ============== KPI Card Component ==============
interface KPICardProps {
  title: string;
  value: string | number;
  change?: number;
  subtitle?: string;
}

function KPICard({ title, value, change, subtitle }: KPICardProps) {
  return (
    <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-100">
      <p className="text-sm font-medium text-gray-500">{title}</p>
      <p className="text-3xl font-bold text-gray-900 mt-2">{value}</p>
      {change !== undefined && (
        <p className={`text-sm mt-2 ${change >= 0 ? 'text-green-600' : 'text-red-600'}`}>
          {formatPercent(change)} vs last month
        </p>
      )}
      {subtitle && (
        <p className="text-sm text-gray-400 mt-1">{subtitle}</p>
      )}
    </div>
  );
}

// ============== Platform Card Component ==============
interface PlatformCardProps {
  platform: PlatformOverview;
}

function PlatformCard({ platform }: PlatformCardProps) {
  const color = getPlatformColor(platform.platform as Platform);
  
  return (
    <div 
      className="bg-white rounded-xl shadow-sm p-5 border-l-4"
      style={{ borderLeftColor: color }}
    >
      <div className="flex justify-between items-start">
        <div>
          <h3 className="font-semibold text-gray-900">
            {getPlatformName(platform.platform as Platform)}
          </h3>
          <p className="text-2xl font-bold mt-1">
            ${formatNumber(parseFloat(platform.total_revenue_usd))}
          </p>
        </div>
        <span 
          className={`text-sm px-2 py-1 rounded ${
            parseFloat(platform.revenue_mom_growth_pct) >= 0 
              ? 'bg-green-100 text-green-700' 
              : 'bg-red-100 text-red-700'
          }`}
        >
          {formatPercent(platform.revenue_mom_growth_pct)}
        </span>
      </div>
      
      <div className="grid grid-cols-3 gap-4 mt-4 text-sm">
        <div>
          <p className="text-gray-500">Orders</p>
          <p className="font-medium">{formatNumber(platform.total_orders)}</p>
        </div>
        <div>
          <p className="text-gray-500">AOV</p>
          <p className="font-medium">${parseFloat(platform.avg_order_value_usd).toFixed(0)}</p>
        </div>
        <div>
          <p className="text-gray-500">Fulfillment</p>
          <p className="font-medium">{platform.fulfillment_rate || 'N/A'}%</p>
        </div>
      </div>
    </div>
  );
}

// ============== Main Dashboard Component ==============
export function Dashboard() {
  const { data: dashboard, loading, error } = useDashboard();
  const { data: dailyData } = useDailySnapshots({ limit: 7 });
  const { data: topProducts } = useProducts({ limit: 5 });

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700">
        <h3 className="font-semibold">Error loading dashboard</h3>
        <p className="text-sm mt-1">{error}</p>
      </div>
    );
  }

  if (!dashboard) return null;

  return (
    <div className="p-6 max-w-7xl mx-auto">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900">Dashboard</h1>
        <p className="text-gray-500 mt-1">E-commerce analytics across all platforms</p>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        <KPICard
          title="Total Revenue"
          value={`$${formatNumber(parseFloat(dashboard.total_revenue_usd))}`}
          change={dashboard.revenue_growth_pct}
        />
        <KPICard
          title="Total Orders"
          value={formatNumber(dashboard.total_orders)}
          change={dashboard.orders_growth_pct}
        />
        <KPICard
          title="Average Order Value"
          value={`$${parseFloat(dashboard.avg_order_value_usd).toFixed(0)}`}
        />
        <KPICard
          title="Active Platforms"
          value={dashboard.platforms.length}
          subtitle="Shopify, Amazon, Lazada, Shopee"
        />
      </div>

      {/* Platform Cards */}
      <div className="mb-8">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Platform Performance</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {dashboard.platforms.map((platform) => (
            <PlatformCard key={platform.platform} platform={platform} />
          ))}
        </div>
      </div>

      {/* Recent Activity & Top Products */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Recent Days */}
        <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-100">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Last 7 Days</h2>
          <div className="space-y-3">
            {dailyData?.map((day) => (
              <div key={day.order_date} className="flex justify-between items-center py-2 border-b border-gray-50">
                <div>
                  <p className="font-medium text-gray-900">
                    {new Date(day.order_date).toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' })}
                  </p>
                  <p className="text-sm text-gray-500">{day.total_orders} orders</p>
                </div>
                <div className="text-right">
                  <p className="font-semibold text-gray-900">
                    ${formatNumber(parseFloat(day.total_revenue_usd))}
                  </p>
                  {day.revenue_dod_change && (
                    <p className={`text-xs ${parseFloat(day.revenue_dod_change) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                      {parseFloat(day.revenue_dod_change) >= 0 ? '↑' : '↓'} ${Math.abs(parseFloat(day.revenue_dod_change)).toFixed(0)}
                    </p>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Top Products */}
        <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-100">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Top Products</h2>
          <div className="space-y-3">
            {topProducts?.map((product, index) => (
              <div key={product.product_id} className="flex items-center gap-4 py-2 border-b border-gray-50">
                <span className="w-6 h-6 rounded-full bg-gray-100 flex items-center justify-center text-sm font-medium text-gray-600">
                  {index + 1}
                </span>
                <div className="flex-1 min-w-0">
                  <p className="font-medium text-gray-900 truncate">{product.product_name}</p>
                  <p className="text-sm text-gray-500">
                    {getPlatformName(product.platform)} · {product.total_units_sold} sold
                  </p>
                </div>
                <div className="text-right">
                  <p className="font-semibold text-gray-900">
                    ${formatNumber(parseFloat(product.total_revenue))}
                  </p>
                  <span 
                    className="text-xs px-2 py-0.5 rounded"
                    style={{ 
                      backgroundColor: getPlatformColor(product.platform) + '20',
                      color: getPlatformColor(product.platform)
                    }}
                  >
                    {product.performance_tier}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

export default Dashboard;

