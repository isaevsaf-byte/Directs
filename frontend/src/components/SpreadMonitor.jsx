import React, { useState, useEffect } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts';
import { fetchCurveData } from '../config/api';

// Format date as "Jan'26"
const formatDate = (dateStr) => {
    const d = new Date(dateStr);
    const month = d.toLocaleString('default', { month: 'short' });
    const year = d.getFullYear().toString().slice(-2);
    return `${month}'${year}`;
};

// Generate fallback spread data based on actual PIX values
const generateFallbackSpreadData = () => {
    // NBSK - BEK spread from Excel data
    const spreadData = [
        { month: "Jan'25", nbsk: 1480.5, bek: 1000, spread: 480.5 },
        { month: "Feb'25", nbsk: 1493.7, bek: 1066.54, spread: 427.16 },
        { month: "Mar'25", nbsk: 1532.1, bek: 1142.4, spread: 389.7 },
        { month: "Apr'25", nbsk: 1573.8, bek: 1195.5, spread: 378.3 },
        { month: "May'25", nbsk: 1597.1, bek: 1193.5, spread: 403.6 },
        { month: "Jun'25", nbsk: 1572.7, bek: 1137.8, spread: 434.9 },
        { month: "Jul'25", nbsk: 1527.8, bek: 1079.7, spread: 448.1 },
        { month: "Aug'25", nbsk: 1499.97, bek: 1013.37, spread: 486.6 },
        { month: "Sep'25", nbsk: 1495.91, bek: 1000, spread: 495.91 },
        { month: "Oct'25", nbsk: 1496.89, bek: 1051.6, spread: 445.29 },
        { month: "Nov'25", nbsk: 1497.58, bek: 1075.19, spread: 422.39 },
        { month: "Dec'25", nbsk: 1498.20, bek: 1096.33, spread: 401.87 },
        // Forward spread
        { month: "Jan'26", nbsk: 1545, bek: 1140, spread: 405 },
        { month: "Feb'26", nbsk: 1545, bek: 1140, spread: 405 },
        { month: "Mar'26", nbsk: 1545, bek: 1140, spread: 405 },
        { month: "Apr'26", nbsk: 1562, bek: 1180, spread: 382 },
        { month: "May'26", nbsk: 1562, bek: 1180, spread: 382 },
        { month: "Jun'26", nbsk: 1562, bek: 1180, spread: 382 },
        { month: "Jul'26", nbsk: 1571, bek: 1204, spread: 367 },
        { month: "Aug'26", nbsk: 1571, bek: 1204, spread: 367 },
        { month: "Sep'26", nbsk: 1571, bek: 1204, spread: 367 },
        { month: "Oct'26", nbsk: 1565, bek: 1230, spread: 335 },
        { month: "Nov'26", nbsk: 1565, bek: 1230, spread: 335 },
    ];

    return spreadData.map(d => ({
        date: d.month,
        spread: Math.round(d.spread),
        nbsk: d.nbsk,
        bek: d.bek
    }));
};

// Calculate spread from two curves
const calculateSpreadFromCurves = (nbskCurve, bekCurve) => {
    const spreadData = [];
    const monthlyData = {};

    // Group NBSK by month
    nbskCurve.forEach(item => {
        const d = new Date(item.date);
        const monthKey = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
        if (!monthlyData[monthKey]) {
            monthlyData[monthKey] = { date: formatDate(item.date), timestamp: d.getTime() };
        }
        // Take first of month or update if we have it
        if (!monthlyData[monthKey].nbsk || d.getDate() === 1) {
            monthlyData[monthKey].nbsk = item.price;
        }
    });

    // Add BEK prices
    bekCurve.forEach(item => {
        const d = new Date(item.date);
        const monthKey = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
        if (monthlyData[monthKey]) {
            if (!monthlyData[monthKey].bek || d.getDate() === 1) {
                monthlyData[monthKey].bek = item.price;
            }
        }
    });

    // Calculate spreads
    Object.values(monthlyData).forEach(item => {
        if (item.nbsk && item.bek) {
            spreadData.push({
                date: item.date,
                spread: Math.round(item.nbsk - item.bek),
                nbsk: Math.round(item.nbsk),
                bek: Math.round(item.bek),
                timestamp: item.timestamp
            });
        }
    });

    // Sort by date
    spreadData.sort((a, b) => a.timestamp - b.timestamp);

    return spreadData;
};

export const SpreadMonitor = () => {
    const [spreadData, setSpreadData] = useState([]);
    const [currentSpread, setCurrentSpread] = useState(0);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [stats, setStats] = useState({ min: 0, max: 0, avg: 0 });

    useEffect(() => {
        const loadData = async () => {
            setLoading(true);
            setError(null);

            try {
                // Fetch both curves
                const [nbskCurve, bekCurve] = await Promise.all([
                    fetchCurveData('NBSK'),
                    fetchCurveData('BEK')
                ]);

                if (nbskCurve && nbskCurve.length > 0 && bekCurve && bekCurve.length > 0) {
                    const data = calculateSpreadFromCurves(nbskCurve, bekCurve);
                    setSpreadData(data);

                    if (data.length > 0) {
                        const spreads = data.map(d => d.spread);
                        setCurrentSpread(spreads[spreads.length - 1] || spreads[0]);
                        setStats({
                            min: Math.min(...spreads),
                            max: Math.max(...spreads),
                            avg: Math.round(spreads.reduce((a, b) => a + b, 0) / spreads.length)
                        });
                    }
                } else {
                    // Fallback to mock data
                    console.warn('API not available, using fallback spread data');
                    const fallbackData = generateFallbackSpreadData();
                    setSpreadData(fallbackData);
                    setError('Using offline data');

                    const spreads = fallbackData.map(d => d.spread);
                    setCurrentSpread(spreads[spreads.length - 1] || spreads[0]);
                    setStats({
                        min: Math.min(...spreads),
                        max: Math.max(...spreads),
                        avg: Math.round(spreads.reduce((a, b) => a + b, 0) / spreads.length)
                    });
                }
            } catch (err) {
                console.error('Error loading spread data:', err);
                setError('Failed to load data');
                const fallbackData = generateFallbackSpreadData();
                setSpreadData(fallbackData);
            }

            setLoading(false);
        };

        loadData();
    }, []);

    const getStatusColor = (val) => {
        // Spread zones based on historical analysis
        if (val < 350) return 'bg-green-500';  // Tight spread - potential buy signal
        if (val > 450) return 'bg-red-500';    // Wide spread - caution
        return 'bg-yellow-500';                 // Normal range
    };

    const getStatusText = (val) => {
        if (val < 350) return 'TIGHT';
        if (val > 450) return 'WIDE';
        return 'NORMAL';
    };

    // Calculate Y-axis domain
    const getYDomain = () => {
        if (spreadData.length === 0) return [300, 550];
        const spreads = spreadData.map(d => d.spread);
        const min = Math.min(...spreads);
        const max = Math.max(...spreads);
        const padding = (max - min) * 0.15;
        return [Math.floor(min - padding), Math.ceil(max + padding)];
    };

    return (
        <div className="p-4 bg-white rounded-lg shadow-md border border-gray-200">
            <div className="flex justify-between items-center mb-4">
                <div>
                    <h2 className="text-lg font-bold text-gray-800">
                        NBSK/BEK Spread Monitor
                        {loading && <span className="ml-2 text-sm text-gray-400">Loading...</span>}
                    </h2>
                    <p className="text-xs text-gray-500">Softwood - Hardwood Price Differential</p>
                    {error && <p className="text-xs text-amber-600">{error}</p>}
                </div>

                <div className="flex items-center space-x-3">
                    {/* Stats */}
                    <div className="text-right text-xs text-gray-500">
                        <div>Min: ${stats.min}</div>
                        <div>Avg: ${stats.avg}</div>
                        <div>Max: ${stats.max}</div>
                    </div>

                    {/* Current Spread Badge */}
                    <div className="text-center">
                        <div className={`px-4 py-2 text-white font-bold rounded-lg ${getStatusColor(currentSpread)}`}>
                            ${currentSpread}
                        </div>
                        <div className="text-xs text-gray-500 mt-1">{getStatusText(currentSpread)}</div>
                    </div>
                </div>
            </div>

            <div className="h-64 w-full">
                <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={spreadData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
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
                            tickFormatter={(value) => `$${value}`}
                            axisLine={false}
                            tickLine={false}
                        />
                        <Tooltip
                            contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)' }}
                            formatter={(value, name, props) => {
                                if (name === 'spread') {
                                    const item = props.payload;
                                    return [
                                        <div key="spread">
                                            <div>Spread: <strong>${value}</strong></div>
                                            <div className="text-xs text-gray-500">NBSK: ${item.nbsk} | BEK: ${item.bek}</div>
                                        </div>,
                                        ''
                                    ];
                                }
                                return [`$${value}`, name];
                            }}
                        />

                        {/* Traffic light thresholds */}
                        <ReferenceLine y={350} stroke="#22c55e" strokeDasharray="3 3" label={{ value: 'Tight', fill: '#22c55e', fontSize: 10 }} />
                        <ReferenceLine y={450} stroke="#ef4444" strokeDasharray="3 3" label={{ value: 'Wide', fill: '#ef4444', fontSize: 10 }} />

                        <Area
                            type="monotone"
                            dataKey="spread"
                            stroke="#8b5cf6"
                            fill="#8b5cf6"
                            fillOpacity={0.3}
                            strokeWidth={2}
                        />
                    </AreaChart>
                </ResponsiveContainer>
            </div>

            {/* Legend */}
            <div className="flex justify-center space-x-6 mt-3 text-xs">
                <div className="flex items-center">
                    <div className="w-3 h-3 rounded-full bg-green-500 mr-1"></div>
                    <span className="text-gray-600">Tight (&lt;$350)</span>
                </div>
                <div className="flex items-center">
                    <div className="w-3 h-3 rounded-full bg-yellow-500 mr-1"></div>
                    <span className="text-gray-600">Normal ($350-450)</span>
                </div>
                <div className="flex items-center">
                    <div className="w-3 h-3 rounded-full bg-red-500 mr-1"></div>
                    <span className="text-gray-600">Wide (&gt;$450)</span>
                </div>
            </div>
        </div>
    );
};
