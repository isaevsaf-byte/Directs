// API Configuration
// Uses environment variable first, then Railway production URL, then localhost for dev

const PRODUCTION_API = 'https://web-production-bbcf1.up.railway.app';

export const API_BASE = import.meta.env.VITE_API_URL
    || (import.meta.env.PROD ? PRODUCTION_API : 'http://localhost:8000');

// Helper to make API calls
export const fetchAPI = async (endpoint, options = {}) => {
    const url = `${API_BASE}${endpoint}`;
    try {
        const response = await fetch(url, {
            ...options,
            headers: {
                'Content-Type': 'application/json',
                ...options.headers,
            },
        });
        if (!response.ok) {
            throw new Error(`API error: ${response.status}`);
        }
        return await response.json();
    } catch (error) {
        console.error(`API call failed: ${endpoint}`, error);
        return null;
    }
};

// Fetch curve data
export const fetchCurveData = async (product = 'NBSK') => {
    return fetchAPI(`/api/v1/market/curve/latest?product=${product}`);
};

// Fetch realized prices
export const fetchRealizedPrices = async (product = 'NBSK', days = 730) => {
    const data = await fetchAPI(`/api/v1/realized/prices?product=${product}&days=${days}`);
    return data?.prices || [];
};
