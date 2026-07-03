// Todo maybe make a deployment branch which can be rebased from main, wich changes this const
//change for deployment
// const BACKEND = 'http://34.28.224.202:5001';
const BACKEND = 'http://localhost:5001';

// consts for path display number of points and time interval for path display
const PATH_LOCATIONS_LIMIT = 30;
const PATH_TIME_INTERVAL = 15; // in minutes

// Fading of stale markers
const STALE_AFTER_MS = 30 * 1000;

// MAX Debug log entries
const DEBUG_LOG_MAX_ENTRIES = 300;

// "now" ticker used for fade calculations
const NOW_UPDATE_INTERVAL_MS = 5000;
// Batch flush interval for taxi position updates
const TAXI_UPDATE_FLUSH_INTERVAL_MS = 3000;


