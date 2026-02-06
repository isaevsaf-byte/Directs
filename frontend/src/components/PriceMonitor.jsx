import React, { useState, useEffect } from 'react';
import { ComposedChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Brush, ReferenceLine } from 'recharts';
import { fetchCurveData, fetchRealizedPrices } from '../config/api';

// Format date as "Jan'26"
const formatDate = (dateStr) => {
    const d = new Date(dateStr);
    const month = d.toLocaleString('default', { month: 'short' });
    const year = d.getFullYear().toString().slice(-2);
    return `${month}'${year}`;
};

// Generate fallback data based on actual PIX values from Excel
const generateFallbackData = (product) => {
    const data = [];

    // Actual PIX prices from Excel for history (12 months: Feb 2025 - Jan 2026)
    const historicalPrices = product === 'NBSK' ? {
        '2025-02': 1493.7, '2025-03': 1532.1, '2025-04': 1573.8,
        '2025-05': 1597.1, '2025-06': 1572.7, '2025-07': 1527.8, '2025-08': 1499.97,
        '2025-09': 1495.91, '2025-10': 1496.89, '2025-11': 1497.58, '2025-12': 1498.20,
        '2026-01': 1545.0
    } : {
        '2025-02': 1066.54, '2025-03': 1142.4, '2025-04': 1195.5,
        '2025-05': 1193.5, '2025-06': 1137.8, '2025-07': 1079.7, '2025-08': 1013.37,
        '2025-09': 1000, '2025-10': 1051.6, '2025-11': 1075.19, '2025-12': 1096.33,
        '2026-01': 1140.0
    };

    // Forward prices from Excel (12 months out to Feb 2027)
    const forwardPrices = product === 'NBSK' ? {
        '2026-02': 1545, '2026-03': 1545, '2026-04': 1562,
        '2026-05': 1562, '2026-06': 1562, '2026-07': 1571, '2026-08': 1571,
        '2026-09': 1571, '2026-10': 1565, '2026-11': 1565,
        '2026-12': 1560, '2027-01': 1558, '2027-02': 1555
    } : {
        '2026-02': 1140, '2026-03': 1140, '2026-04': 1180,
        '2026-05': 1180, '2026-06': 1180, '2026-07': 1204, '2026-08': 1204,
        '2026-09': 1204, '2026-10': 1230, '2026-11': 1230,
        '2026-12': 1245, '2027-01': 1255, '2027-02': 1260
    };

    // Add historical data
    Object.entries(historicalPrices).forEach(([monthKey, price]) => {
        const [year, month] = monthKey.split('-');
        const d = new Date(parseInt(year), parseInt(month) - 1, 15);
        data.push({
            date: formatDate(d),
            type: 'actual',
            actualPrice: Math.round(price),
            forecastPrice: null,
            timestamp: d.getTime()
        });
    });

    // Add forward data
    Object.entries(forwardPrices).forEach(([monthKey, price]) => {
        const [year, month] = monthKey.split('-');
        const d = new Date(parseInt(year), parseInt(month) - 1, 15);
        data.push({
            date: formatDate(d),
            type: 'forecast',
            actualPrice: null,
            forecastPrice: Math.round(price),
            timestamp: d.getTime()
        });
    });

    // Sort by timestamp
    data.sort((a, b) => a.timestamp - b.timestamp);

    // Add connector point where history meets forecast
    const lastHistorical = data.filter(d => d.type === 'actual').pop();
    if (lastHistorical) {
        lastHistorical.forecastPrice = lastHistorical.actualPrice;
    }

    return data;
};

// Process API data into chart format
// Ensures history and forecast lines don't overlap:
//   - actualPrice: only for the last 12 months up to and including this month
//   - forecastPrice: only for months from this month onward (12 months forward)
//   - They share one "connector" month so the lines visually meet
const processApiData = (realizedPrices, forecastCurve) => {
    const monthlyData = {};
    const now = new Date();
    const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;

    // Calculate 12 months ago cutoff for history
    const twelveMonthsAgo = new Date(now.getFullYear(), now.getMonth() - 11, 1);
    const historyStartKey = `${twelveMonthsAgo.getFullYear()}-${String(twelveMonthsAgo.getMonth() + 1).padStart(2, '0')}`;

    // Process realized prices (history) — only last 12 months up to current month
    if (realizedPrices && realizedPrices.length > 0) {
        realizedPrices.forEach(item => {
            const d = new Date(item.date);
            const monthKey = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;

            // Skip future realized prices (shouldn't exist, but be safe)
            if (monthKey > currentMonthKey) return;

            // Skip history older than 12 months
            if (monthKey < historyStartKey) return;

            if (!monthlyData[monthKey] || d.getDate() === 15) {
                monthlyData[monthKey] = {
                    date: formatDate(item.date),
                    type: 'actual',
                    actualPrice: Math.round(item.price),
                    forecastPrice: null,
                    timestamp: d.getTime()
                };
            }
        });
    }

    // Process forecast curve — only current month onward
    if (forecastCurve && forecastCurve.length > 0) {
        // Pick one price per month (first of month or mid-month)
        const monthlyForecast = {};
        forecastCurve.forEach(item => {
            const d = new Date(item.date);
            const monthKey = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;

            // Only include current month and future
            if (monthKey < currentMonthKey) return;

            if (!monthlyForecast[monthKey] || d.getDate() === 1 || d.getDate() === 15) {
                monthlyForecast[monthKey] = { date: item.date, price: item.price };
            }
        });

        Object.entries(monthlyForecast).forEach(([monthKey, { date: itemDate, price }]) => {
            const d = new Date(itemDate);

            if (!monthlyData[monthKey]) {
                // Pure forecast month (no history here)
                monthlyData[monthKey] = {
                    date: formatDate(itemDate),
                    type: 'forecast',
                    actualPrice: null,
                    forecastPrice: Math.round(price),
                    timestamp: d.getTime()
                };
            } else if (monthlyData[monthKey].type === 'actual') {
                // Connector month: history meets forecast
                monthlyData[monthKey].forecastPrice = Math.round(price);
            }
        });
    }

    // Convert to array and sort
    const data = Object.values(monthlyData);
    data.sort((a, b) => a.timestamp - b.timestamp);

    return data;
};

export const PriceMonitor = ({ product, color }) => {
    const [data, setData] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [stats, setStats] = useState({ current: 0, min: 0, max: 0 });
    const [brushRange, setBrushRange] = useState({ startIndex: 0, endIndex: 0 });

    useEffect(() => {
        const loadData = async () => {
            setLoading(true);
            setError(null);

            try {
                const [realizedPrices, forecastCurve] = await Promise.all([
                    fetchRealizedPrices(product),
                    fetchCurveData(product)
                ]);

                let processedData;
                if (forecastCurve && forecastCurve.length > 0) {
                    processedData = processApiData(realizedPrices, forecastCurve);
                } else {
                    console.warn(`${product}: API not available, using fallback data`);
                    processedData = generateFallbackData(product);
                    setError('Using offline data');
                }

                setData(processedData);
                setBrushRange({ startIndex: 0, endIndex: processedData.length - 1 });

                // Calculate stats
                const forecastPrices = processedData
                    .filter(d => d.forecastPrice)
                    .map(d => d.forecastPrice);

                if (forecastPrices.length > 0) {
                    setStats({
                        current: forecastPrices[0],
                        min: Math.min(...forecastPrices),
                        max: Math.max(...forecastPrices)
                    });
                }
            } catch (err) {
                console.error(`Error loading ${product} data:`, err);
                setError('Failed to load data');
                const fallbackData = generateFallbackData(product);
                setData(fallbackData);
                setBrushRange({ startIndex: 0, endIndex: fallbackData.length - 1 });
            }

            setLoading(false);
        };

        loadData();
    }, [product]);

    // Calculate Y-axis domain based on data
    const getYDomain = () => {
        if (data.length === 0) return product === 'NBSK' ? [1400, 1700] : [900, 1300];
        const allPrices = data.flatMap(d => [d.actualPrice, d.forecastPrice].filter(Boolean));
        if (allPrices.length === 0) return product === 'NBSK' ? [1400, 1700] : [900, 1300];
        const min = Math.min(...allPrices);
        const max = Math.max(...allPrices);
        const padding = (max - min) * 0.1;
        return [Math.floor(min - padding), Math.ceil(max + padding)];
    };

    const getTodayLabel = () => {
        const today = new Date();
        return formatDate(today);
    };

    const handleBrushChange = (e) => {
        if (e && e.startIndex !== undefined) {
            setBrushRange({ startIndex: e.startIndex, endIndex: e.endIndex });
        }
    };

    return (
        <div className="bg-white p-4 rounded-lg shadow-md border border-gray-200">
            <div className="flex flex-col sm:flex-row justify-between items-center mb-4">
                <div>
                    <h2 className="text-lg font-bold text-gray-800">
                        {product} Price Monitor
                        {loading && <span className="ml-2 text-sm text-gray-400">Loading...</span>}
                    </h2>
                    <p className="text-xs text-gray-500 uppercase tracking-wide">
                        {product === 'NBSK' ? 'Softwood (DAP Europe)' : 'Hardwood/Eucalyptus (Europe)'}
                    </p>
                    {error && <p className="text-xs text-amber-600 mt-1">{error}</p>}
                </div>

                <div className="flex space-x-4 mt-2 sm:mt-0">
                    <div className="text-center">
                        <div className="text-xs text-gray-500">Current</div>
                        <div className="text-lg font-bold" style={{ color }}>${stats.current}</div>
                    </div>
                    <div className="text-center">
                        <div className="text-xs text-gray-500">Range</div>
                        <div className="text-sm font-medium text-gray-700">
                            ${stats.min} - ${stats.max}
                        </div>
                    </div>
                </div>
            </div>

            <div className="h-80 w-full">
                <ResponsiveContainer width="100%" height="100%">
                    <ComposedChart data={data} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f3f4f6" />
                        <XAxis
                            dataKey="date"
                            tick={{ fontSize: 11, fill: '#9ca3af' }}
                            tickLine={false}
                            axisLine={false}
                            minTickGap={30}
                        />
                        <YAxis
                            domain={getYDomain()}
                            tick={{ fontSize: 12, fill: '#6b7280' }}
                            axisLine={false}
                            tickLine={false}
                            tickFormatter={(value) => `$${value}`}
                        />
                        <Tooltip
                            contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)' }}
                            labelStyle={{ color: '#374151', fontWeight: 'bold' }}
                            formatter={(value, name) => [`$${value}`, name]}
                        />
                        <Legend verticalAlign="top" iconType="circle" height={36} />

                        <ReferenceLine x={getTodayLabel()} stroke="#d1d5db" strokeDasharray="3 3" label="Today" />

                        <Line
                            type="monotone"
                            dataKey="actualPrice"
                            name="History (PIX Actuals)"
                            stroke="#1f2937"
                            strokeWidth={2.5}
                            dot={false}
                            connectNulls={false}
                        />

                        <Line
                            type="monotone"
                            dataKey="forecastPrice"
                            name="Forecast (Forward Curve)"
                            stroke={color}
                            strokeWidth={3}
                            dot={false}
                            activeDot={{ r: 6, strokeWidth: 0 }}
                        />

                        <Brush
                            dataKey="date"
                            height={25}
                            stroke="#e5e7eb"
                            fill="#f9fafb"
                            tickFormatter={() => ''}
                            startIndex={brushRange.startIndex}
                            endIndex={brushRange.endIndex}
                            onChange={handleBrushChange}
                        />
                    </ComposedChart>
                </ResponsiveContainer>
            </div>
        </div>
    );
};
