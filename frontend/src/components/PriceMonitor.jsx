import React, { useState, useEffect, useMemo } from 'react';
import { ComposedChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Brush, ReferenceLine } from 'recharts';
import { fetchCurveData, fetchRealizedPrices } from '../config/api';

// Format date as "Jan'26"
const formatDate = (dateStr) => {
    const d = new Date(dateStr);
    const month = d.toLocaleString('default', { month: 'short' });
    const year = d.getFullYear().toString().slice(-2);
    return `${month}'${year}`;
};

// Time range options
const TIME_RANGES = [
    { label: '1Y', months: 12 },
    { label: '2Y', months: 24 },
    { label: '3Y', months: 36 },
    { label: 'All', months: null },
];

// Process API data into chart format with configurable history length
const processApiData = (realizedPrices, forecastCurve, historyMonths = null) => {
    const monthlyData = {};
    const now = new Date();
    const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;

    // Calculate history start cutoff (null = no limit)
    let historyStartKey = null;
    if (historyMonths) {
        const cutoffDate = new Date(now.getFullYear(), now.getMonth() - historyMonths + 1, 1);
        historyStartKey = `${cutoffDate.getFullYear()}-${String(cutoffDate.getMonth() + 1).padStart(2, '0')}`;
    }

    // Process realized prices (history)
    if (realizedPrices && realizedPrices.length > 0) {
        realizedPrices.forEach(item => {
            const d = new Date(item.date);
            const monthKey = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;

            // Skip future realized prices
            if (monthKey > currentMonthKey) return;

            // Skip history older than cutoff (if set)
            if (historyStartKey && monthKey < historyStartKey) return;

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
        const monthlyForecast = {};
        forecastCurve.forEach(item => {
            const d = new Date(item.date);
            const monthKey = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;

            if (monthKey < currentMonthKey) return;

            if (!monthlyForecast[monthKey] || d.getDate() === 1 || d.getDate() === 15) {
                monthlyForecast[monthKey] = { date: item.date, price: item.price };
            }
        });

        Object.entries(monthlyForecast).forEach(([monthKey, { date: itemDate, price }]) => {
            const d = new Date(itemDate);

            if (!monthlyData[monthKey]) {
                monthlyData[monthKey] = {
                    date: formatDate(itemDate),
                    type: 'forecast',
                    actualPrice: null,
                    forecastPrice: Math.round(price),
                    timestamp: d.getTime()
                };
            } else if (monthlyData[monthKey].type === 'actual') {
                monthlyData[monthKey].forecastPrice = Math.round(price);
            }
        });
    }

    const data = Object.values(monthlyData);
    data.sort((a, b) => a.timestamp - b.timestamp);
    return data;
};

export const PriceMonitor = ({ product, color }) => {
    const [rawData, setRawData] = useState({ realized: [], forecast: [] });
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [timeRange, setTimeRange] = useState('1Y');
    const [stats, setStats] = useState({ current: 0, min: 0, max: 0 });

    // Load data once on mount
    useEffect(() => {
        const loadData = async () => {
            setLoading(true);
            setError(null);

            try {
                const [realizedPrices, forecastCurve] = await Promise.all([
                    fetchRealizedPrices(product, 2000), // Get all available history
                    fetchCurveData(product)
                ]);

                if (forecastCurve && forecastCurve.length > 0) {
                    setRawData({ realized: realizedPrices || [], forecast: forecastCurve });
                } else {
                    setError('Using offline data');
                    setRawData({ realized: [], forecast: [] });
                }
            } catch (err) {
                console.error(`Error loading ${product} data:`, err);
                setError('Failed to load data');
            }

            setLoading(false);
        };

        loadData();
    }, [product]);

    // Process data based on selected time range
    const data = useMemo(() => {
        const selectedRange = TIME_RANGES.find(r => r.label === timeRange);
        const historyMonths = selectedRange?.months;
        return processApiData(rawData.realized, rawData.forecast, historyMonths);
    }, [rawData, timeRange]);

    // Update stats when data changes
    useEffect(() => {
        const forecastPrices = data
            .filter(d => d.forecastPrice)
            .map(d => d.forecastPrice);

        if (forecastPrices.length > 0) {
            setStats({
                current: forecastPrices[0],
                min: Math.min(...forecastPrices),
                max: Math.max(...forecastPrices)
            });
        }
    }, [data]);

    // Calculate Y-axis domain based on visible data
    const getYDomain = () => {
        if (data.length === 0) return product === 'NBSK' ? [650, 900] : [500, 750];
        const allPrices = data.flatMap(d => [d.actualPrice, d.forecastPrice].filter(Boolean));
        if (allPrices.length === 0) return product === 'NBSK' ? [650, 900] : [500, 750];
        const min = Math.min(...allPrices);
        const max = Math.max(...allPrices);
        const padding = (max - min) * 0.1;
        return [Math.floor(min - padding), Math.ceil(max + padding)];
    };

    const getTodayLabel = () => formatDate(new Date());

    // Get date range for display
    const getDateRangeLabel = () => {
        if (data.length === 0) return '';
        const actualData = data.filter(d => d.actualPrice);
        const forecastData = data.filter(d => d.forecastPrice && !d.actualPrice);

        if (actualData.length === 0 && forecastData.length === 0) return '';

        const firstDate = data[0]?.date || '';
        const lastDate = data[data.length - 1]?.date || '';
        return `${firstDate} → ${lastDate}`;
    };

    return (
        <div className="bg-white p-4 rounded-lg shadow-md border border-gray-200">
            {/* Header */}
            <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-4 gap-2">
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

                <div className="flex items-center space-x-4">
                    {/* Time Range Selector */}
                    <div className="flex bg-gray-100 rounded-lg p-0.5">
                        {TIME_RANGES.map(range => (
                            <button
                                key={range.label}
                                onClick={() => setTimeRange(range.label)}
                                className={`px-3 py-1 text-xs font-medium rounded-md transition-colors ${
                                    timeRange === range.label
                                        ? 'bg-white text-gray-900 shadow-sm'
                                        : 'text-gray-600 hover:text-gray-900'
                                }`}
                            >
                                {range.label}
                            </button>
                        ))}
                    </div>

                    {/* Stats */}
                    <div className="flex space-x-3">
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
            </div>

            {/* Date range indicator */}
            <div className="text-xs text-gray-400 mb-2 text-right">
                {getDateRangeLabel()}
            </div>

            {/* Chart */}
            <div className="h-80 w-full">
                <ResponsiveContainer width="100%" height="100%">
                    <ComposedChart data={data} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f3f4f6" />
                        <XAxis
                            dataKey="date"
                            tick={{ fontSize: 11, fill: '#9ca3af' }}
                            tickLine={false}
                            axisLine={false}
                            minTickGap={40}
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
                            strokeWidth={2}
                            dot={false}
                            connectNulls={false}
                        />

                        <Line
                            type="monotone"
                            dataKey="forecastPrice"
                            name="Forecast (Forward Curve)"
                            stroke={color}
                            strokeWidth={2.5}
                            dot={false}
                            activeDot={{ r: 5, strokeWidth: 0 }}
                        />

                        <Brush
                            dataKey="date"
                            height={25}
                            stroke="#e5e7eb"
                            fill="#f9fafb"
                            tickFormatter={() => ''}
                        />
                    </ComposedChart>
                </ResponsiveContainer>
            </div>
        </div>
    );
};
