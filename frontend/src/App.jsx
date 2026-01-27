import React from 'react';
import { PriceMonitor } from './components/PriceMonitor';
import { SpreadMonitor } from './components/SpreadMonitor';

function App() {
    return (
        <div className="min-h-screen bg-gray-50 p-6 font-sans">
            <header className="mb-8 border-b border-gray-200 pb-4">
                <h1 className="text-3xl font-extrabold text-gray-900 tracking-tight">Pulp Market Intelligence Hub</h1>
                <p className="text-sm font-medium text-gray-500 mt-1">Global Fiber Derivatives • 2026 Protocol</p>
            </header>

            <main className="space-y-8">

                {/* Row 1: The "Different Windows" - NBSK & BEK Side-by-Side */}
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {/* Card A: NBSK (Softwood) */}
                    <PriceMonitor product="NBSK" color="#2563eb" /> {/* Electric Blue */}

                    {/* Card B: BEK (Hardwood) */}
                    <PriceMonitor product="BEK" color="#10b981" /> {/* Emerald Green */}
                </div>

                {/* Row 2: Spread & Status */}
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {/* Spread Monitor */}
                    <SpreadMonitor />

                    {/* System Status / Diagnostics */}
                    <section className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
                        <h3 className="text-lg font-bold text-gray-800 mb-4">System Diagnostics</h3>
                        <div className="space-y-3">
                            <div className="flex justify-between items-center p-3 bg-gray-50 rounded">
                                <span className="text-sm text-gray-600">Scraper Status</span>
                                <span className="px-2 py-1 text-xs font-bold text-green-700 bg-green-100 rounded-full">LIVE</span>
                            </div>
                            <div className="flex justify-between items-center p-3 bg-gray-50 rounded">
                                <span className="text-sm text-gray-600">Index Protocol</span>
                                <span className="text-sm font-mono text-gray-800">NBSK / BEK 2026</span>
                            </div>
                            <div className="flex justify-between items-center p-3 bg-gray-50 rounded">
                                <span className="text-sm text-gray-600">Forecast Accuracy</span>
                                <span className="px-2 py-1 text-xs font-bold text-blue-700 bg-blue-100 rounded-full">ACTIVE</span>
                            </div>
                        </div>
                    </section>
                </div>
            </main>
        </div>
    );
}

export default App;
