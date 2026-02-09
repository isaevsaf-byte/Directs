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
                {/* Row 3: How It Works Legend */}
                <section className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
                    <h3 className="text-lg font-bold text-gray-800 mb-4">How This Dashboard Works</h3>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6 text-sm text-gray-600">

                        {/* Data Sources */}
                        <div>
                            <h4 className="font-semibold text-gray-800 mb-2 flex items-center gap-2">
                                <span className="w-2 h-2 rounded-full bg-gray-800 inline-block"></span>
                                Historical Data (Black Line)
                            </h4>
                            <p className="mb-2">
                                <strong>Source:</strong> Fastmarkets PIX indices — the industry-standard benchmark
                                for global pulp pricing, published by Fastmarkets RISI.
                            </p>
                            <p>
                                <strong>What we do:</strong> Monthly PIX settlement prices for NBSK (softwood) and
                                BEK (hardwood) are collected and stored. Forecast accuracy is automatically
                                validated against new PIX actuals every <strong>Tuesday at 08:00 UTC</strong>.
                            </p>
                        </div>

                        {/* Forward Curve */}
                        <div>
                            <h4 className="font-semibold text-gray-800 mb-2 flex items-center gap-2">
                                <span className="w-2 h-2 rounded-full bg-blue-500 inline-block"></span>
                                Forward Curve (Colored Line)
                            </h4>
                            <p className="mb-2">
                                <strong>Source:</strong> CME/NOREXECO traded pulp derivatives — real settlement prices
                                from monthly futures contracts, scraped automatically <strong>daily at 18:00 UTC</strong> after
                                European market close.
                            </p>
                            <p>
                                <strong>What we do:</strong> Monthly contract prices are fed through a
                                <strong> maximum smoothness spline</strong> algorithm to generate a smooth daily curve.
                                The spline minimizes curvature while ensuring the average price in each contract period
                                matches the traded settlement level — <em>arbitrage-free</em> curve construction.
                            </p>
                        </div>

                        {/* Ensemble Forecast */}
                        <div>
                            <h4 className="font-semibold text-gray-800 mb-2 flex items-center gap-2">
                                <span className="w-2 h-2 rounded-full bg-orange-500 inline-block"></span>
                                Ensemble Forecast
                            </h4>
                            <p className="mb-2">
                                Three models are combined with horizon-adjusted weights:
                                <strong> Futures Curve</strong> (market-implied prices),
                                <strong> ARIMA</strong> (momentum and autoregression), and
                                <strong> Mean Reversion</strong> (pull toward long-term equilibrium).
                            </p>
                            <p>
                                Near-term (&lt;30d) trusts futures most (60%), long-term (&gt;90d) leans on
                                mean reversion (40%). Predictions are stored daily and scored against
                                PIX actuals to track MAPE and bias.
                            </p>
                        </div>

                        {/* Spread */}
                        <div>
                            <h4 className="font-semibold text-gray-800 mb-2 flex items-center gap-2">
                                <span className="w-2 h-2 rounded-full bg-purple-500 inline-block"></span>
                                Spread Monitor
                            </h4>
                            <p>
                                The spread is NBSK minus BEK from the forward curves.
                                <strong> Tight</strong> (&lt;$350) = prices converging,
                                <strong> Normal</strong> ($350–450) = typical range,
                                <strong> Wide</strong> (&gt;$450) = divergence.
                            </p>
                        </div>

                        {/* Automated Pipeline */}
                        <div className="md:col-span-2 border-t border-gray-100 pt-4">
                            <h4 className="font-semibold text-gray-800 mb-2 flex items-center gap-2">
                                <span className="w-2 h-2 rounded-full bg-green-500 inline-block"></span>
                                Automated Pipeline
                            </h4>
                            <p>
                                <strong>Daily 18:00 UTC:</strong> Scrape NOREXECO contracts → Build spline curves →
                                Run ensemble forecast → Validate against actuals.
                                <strong> Weekly Tue 08:00 UTC:</strong> Re-validate pending forecasts against
                                newly published PIX prices. All snapshots are timestamped for "time machine" queries.
                            </p>
                        </div>
                    </div>
                </section>

            </main>
        </div>
    );
}

export default App;
