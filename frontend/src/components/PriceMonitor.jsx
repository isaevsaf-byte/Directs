import React, { useState, useEffect } from 'react';
import { ComposedChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Brush, ReferenceLine } from 'recharts';

// Mock Data Generator (Simulates "Actuals" + "Forecast")
const generateMarketData = (product) => {
    const data = [];
    const today = new Date('2026-02-01');
    const historyMonths = 24;
    const forecastMonths = 24;

    // Base price helpers
    const base = product === 'NBSK' ? 950 : 700; // BEK is cheaper
    const volatility = product === 'NBSK' ? 20 : 15;

    // 1. Generate History (Actuals) - Past 24 Months
    for (let i = historyMonths; i > 0; i--) {
        const d = new Date(today);
        d.setMonth(d.getMonth() - i);
        const dateStr = d.toLocaleString('default', { month: 'short', year: '2-digit' }); // Jan'24

        // Random walk for history
        const price = base - (i * 5) + (Math.random() * volatility * 2);

        data.push({
            date: dateStr,
            type: 'actual',
            actualPrice: Math.round(price),
            forecastPrice: null, // No forecast in past
            timestamp: d.getTime() // For sorting/brush
        });
    }

    // 2. Connector Point (Today) - Both lines meet here
    const todayStr = today.toLocaleString('default', { month: 'short', year: '2-digit' });
    const currentPrice = base; // Starting point for forecast
    data.push({
        date: todayStr,
        type: 'actual',
        actualPrice: currentPrice,
        forecastPrice: currentPrice, // Connects the lines
        timestamp: today.getTime()
    });

    // 3. Generate Forecast (Futures) - Next 24 Months
    for (let i = 1; i <= forecastMonths; i++) {
        const d = new Date(today);
        d.setMonth(d.getMonth() + i);
        const dateStr = d.toLocaleString('default', { month: 'short', year: '2-digit' });

        // Curve shape
        const price = base + (i * 10) + (Math.sin(i / 4) * volatility);

        data.push({
            date: dateStr,
            type: 'forecast',
            actualPrice: null,
            forecastPrice: Math.round(price),
            timestamp: d.getTime()
        });
    }

    return data;
};

export const PriceMonitor = ({ product, color }) => {
    const [data, setData] = useState([]);
    // State for controlling the zoom window (Brush)
    const [brushRange, setBrushRange] = useState({ startIndex: 0, endIndex: 0 });

    useEffect(() => {
        const mock = generateMarketData(product);
        setData(mock);
        // Initialize showing everything
        setBrushRange({ startIndex: 0, endIndex: mock.length - 1 });
    }, [product]);

    // Helper to find the "Today" index (the connector point)
    const getTodayIndex = () => data.findIndex(d => d.date.includes("Feb'26"));

    const handlePreset = (range) => {
        if (!data.length) return;

        const total = data.length;
        const lastIdx = total - 1;

        if (range === 'ALL') {
            setBrushRange({ startIndex: 0, endIndex: lastIdx });
            return;
        }

        // Logic: Center the view around "Today" for context
        const todayIdx = getTodayIndex();
        if (todayIdx === -1) return;

        let offset = 0;
        if (range === '2Y') offset = 12; // 12 back, 12 forward
        if (range === '1Y') offset = 6;  // 6 back, 6 forward
        if (range === '6M') offset = 3;  // 3 back, 3 forward

        let start = todayIdx - offset;
        let end = todayIdx + offset;

        // Clamp
        if (start < 0) start = 0;
        if (end > lastIdx) end = lastIdx;

        setBrushRange({ startIndex: start, endIndex: end });
    };

    const handleBrushChange = (e) => {
        // Keep state in sync when user drags
        if (e && e.startIndex !== undefined) {
            setBrushRange({ startIndex: e.startIndex, endIndex: e.endIndex });
        }
    };

    return (
        <div className="bg-white p-4 rounded-lg shadow-md border border-gray-200">
            {/* Header with Controls */}
            <div className="flex flex-col sm:flex-row justify-between items-center mb-4">
                <div>
                    <h2 className="text-lg font-bold text-gray-800">{product} Price Monitor</h2>
                    <p className="text-xs text-gray-500 uppercase tracking-wide">
                        Actuals (Dark) vs Forecast ({color === '#2563eb' ? 'Blue' : 'Green'})
                    </p>
                </div>

                {/* Preset Buttons */}
                <div className="flex space-x-1 mt-2 sm:mt-0">
                    {['6M', '1Y', '2Y', 'ALL'].map((label) => (
                        <button
                            key={label}
                            onClick={() => handlePreset(label)}
                            className="px-3 py-1 text-xs font-semibold bg-gray-100 hover:bg-gray-200 text-gray-700 rounded transition-colors active:bg-gray-300 focus:outline-none focus:ring-2 focus:ring-offset-1 focus:ring-blue-500"
                        >
                            {label}
                        </button>
                    ))}
                </div>
            </div>

            {/* Chart Area */}
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
                        <YAxis domain={['auto', 'auto']} tick={{ fontSize: 12, fill: '#6b7280' }} axisLine={false} tickLine={false} />
                        <Tooltip
                            contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)' }}
                            labelStyle={{ color: '#374151', fontWeight: 'bold' }}
                        />
                        <Legend verticalAlign="top" iconType="circle" height={36} />

                        {/* Reference Line for "Today" */}
                        <ReferenceLine x="Feb'26" stroke="#d1d5db" strokeDasharray="3 3" label="Today" />

                        {/* Series 1: Actuals (History) - Dark Line */}
                        <Line
                            type="monotone"
                            dataKey="actualPrice"
                            name="History (Actuals)"
                            stroke="#1f2937" // Gray-800
                            strokeWidth={2.5}
                            dot={false}
                            connectNulls={false}
                        />

                        {/* Series 2: Forecast (Futures) - Bright Line */}
                        <Line
                            type="monotone"
                            dataKey="forecastPrice"
                            name="Forecast (Futures)"
                            stroke={color}
                            strokeWidth={3}
                            dot={false}
                            activeDot={{ r: 6, strokeWidth: 0 }}
                        />

                        {/* Zoom Slider (Controlled) */}
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
