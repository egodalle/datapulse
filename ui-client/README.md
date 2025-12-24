# DataPulse UI Client

API client code for integrating the DataPulse API with your Lovable UI.

## Files

| File | Description |
|------|-------------|
| `src/types.ts` | TypeScript type definitions for all API responses |
| `src/api.ts` | API client with all endpoints |
| `src/hooks.ts` | React hooks for data fetching |
| `src/components/DashboardExample.tsx` | Example dashboard component |

## Installation

### Step 1: Copy files to your Lovable project

Copy the contents of the `src/` folder to your Lovable project:

```
your-lovable-project/
├── src/
│   ├── api/
│   │   ├── types.ts      ← Copy from ui-client/src/types.ts
│   │   ├── api.ts        ← Copy from ui-client/src/api.ts
│   │   └── hooks.ts      ← Copy from ui-client/src/hooks.ts
│   └── components/
│       └── Dashboard.tsx ← Copy from ui-client/src/components/DashboardExample.tsx
```

### Step 2: Configure API URL

Set your API URL in your environment:

**For local development:**
Create a `.env` file:
```
VITE_API_URL=http://localhost:6000
```

**For ngrok tunnel:**
```
VITE_API_URL=https://your-tunnel.ngrok-free.app
```

**For production:**
```
VITE_API_URL=https://your-deployed-api.com
```

Or modify the `API_BASE_URL` directly in `api.ts`:
```typescript
const API_BASE_URL = 'https://your-api-url.com';
```

## Usage

### Using the API client directly

```typescript
import { datapulseAPI } from './api/api';

// Get dashboard data
const dashboard = await datapulseAPI.getDashboard();
console.log(dashboard.total_revenue_usd);

// Get platform overview
const platforms = await datapulseAPI.getPlatforms();

// Get daily snapshots
const dailyData = await datapulseAPI.getDailySnapshots({ limit: 7 });

// Get products by platform
const shopifyProducts = await datapulseAPI.getProducts({ 
  platform: 'shopify', 
  limit: 10 
});
```

### Using React hooks

```tsx
import { useDashboard, usePlatforms, useProducts } from './api/hooks';

function MyDashboard() {
  const { data, loading, error, refetch } = useDashboard();

  if (loading) return <div>Loading...</div>;
  if (error) return <div>Error: {error}</div>;

  return (
    <div>
      <h1>Total Revenue: ${data.total_revenue_usd}</h1>
      <p>Total Orders: {data.total_orders}</p>
      <button onClick={refetch}>Refresh</button>
    </div>
  );
}
```

### Using auto-refresh for real-time data

```tsx
import { useLiveDashboard } from './api/hooks';

function LiveDashboard() {
  // Auto-refreshes every 60 seconds
  const { data, loading, pause, resume } = useLiveDashboard(60000);

  return (
    <div>
      {/* Your dashboard UI */}
      <button onClick={pause}>Pause updates</button>
      <button onClick={resume}>Resume updates</button>
    </div>
  );
}
```

## API Endpoints

| Hook | Endpoint | Description |
|------|----------|-------------|
| `useDashboard()` | `/api/v1/kpis/dashboard` | Main dashboard summary |
| `usePlatforms()` | `/api/v1/kpis/platforms` | Platform overview |
| `useDailySnapshots()` | `/api/v1/kpis/daily` | Daily KPI snapshots |
| `useRevenue()` | `/api/v1/kpis/revenue` | Revenue by platform |
| `useProducts()` | `/api/v1/kpis/products` | Product performance |
| `useStores()` | `/api/v1/stores/` | Store connections |

## Helper Functions

```typescript
import { 
  formatPercent, 
  formatNumber, 
  getPlatformName, 
  getPlatformColor 
} from './api/hooks';

formatPercent(5.5);      // "+5.5%"
formatPercent(-3.2);     // "-3.2%"
formatNumber(1500000);   // "1.5M"
formatNumber(45000);     // "45K"
getPlatformName('shopify'); // "Shopify"
getPlatformColor('shopify'); // "#96bf48"
```

## Troubleshooting

### CORS Errors
Your API is already configured to accept requests from `https://datapulsestore.lovable.app`. If you're getting CORS errors, make sure:
1. The API is running
2. You're using the correct API URL
3. The request includes proper headers

### Network Errors
1. Check if the API is running: `curl http://localhost:6000/health`
2. If using ngrok, make sure the tunnel is active
3. Check browser console for detailed error messages

## Example Response Data

### Dashboard Summary
```json
{
  "total_revenue_usd": "2437936.12",
  "total_orders": 4000,
  "avg_order_value_usd": "609.48",
  "revenue_growth_pct": -26.35,
  "orders_growth_pct": -21.91,
  "platforms": [...],
  "recent_days": [...]
}
```

### Platform Overview
```json
{
  "platform": "shopify",
  "total_orders": 1000,
  "total_revenue_usd": "932128.54",
  "orders_this_month": 118,
  "revenue_mom_growth_pct": "-35.78"
}
```

