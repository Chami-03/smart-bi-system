const chartRegistry = {};

function destroyChart(chartId) {
    if (chartRegistry[chartId]) {
        chartRegistry[chartId].destroy();
    }
}

function createLineChart(canvasId, labels, datasetLabel, data, color) {
    destroyChart(canvasId);
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;

    chartRegistry[canvasId] = new Chart(ctx, {
        type: "line",
        data: {
            labels,
            datasets: [{
                label: datasetLabel,
                data,
                borderColor: color,
                backgroundColor: `${color}33`,
                fill: true,
                tension: 0.3,
                borderWidth: 2,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: true } },
            scales: { y: { beginAtZero: true } },
        },
    });
}

function createBarChart(canvasId, labels, datasetLabel, data, color) {
    destroyChart(canvasId);
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;

    chartRegistry[canvasId] = new Chart(ctx, {
        type: "bar",
        data: {
            labels,
            datasets: [{
                label: datasetLabel,
                data,
                backgroundColor: color,
                borderRadius: 8,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: true } },
            scales: { y: { beginAtZero: true } },
        },
    });
}

function createPieChart(canvasId, labels, data) {
    destroyChart(canvasId);
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;

    chartRegistry[canvasId] = new Chart(ctx, {
        type: "pie",
        data: {
            labels,
            datasets: [{
                data,
                backgroundColor: [
                    "#1f4e8c",
                    "#2b6cb0",
                    "#4092d0",
                    "#6bb3de",
                    "#8fd2b8",
                    "#b8dfc5",
                    "#f2b880",
                    "#eb6f6f",
                    "#9ea9b3",
                    "#7c8a99",
                ],
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: "bottom" } },
        },
    });
}

function renderSalesTrend(monthlySales) {
    const rows = Array.isArray(monthlySales) ? monthlySales : [];
    const labels = rows.map((row) => row.month || "N/A");
    const values = rows.map((row) => Number(row.revenue || 0));
    if (!labels.length) {
        createLineChart("salesTrendChart", ["No Data"], "Revenue", [0], "#1f4e8c");
        return;
    }
    createLineChart("salesTrendChart", labels, "Revenue", values, "#1f4e8c");
}

function renderTopProducts(topProducts) {
    const rows = Array.isArray(topProducts) ? topProducts : [];
    const labels = rows.map((row) => row.product || "N/A");
    const values = rows.map((row) => Number(row.revenue || 0));
    if (!labels.length) {
        createBarChart("topProductsChart", ["No Data"], "Revenue", [0], "#2b6cb0");
        return;
    }
    createBarChart("topProductsChart", labels, "Revenue", values, "#2b6cb0");
}

function renderProductDemand(productDemand) {
    const labels = productDemand.map((row) => row.product);
    const values = productDemand.map((row) => row.quantity);
    createPieChart("productDemandChart", labels, values);
}

function renderCustomerSpending(customerSpending) {
    const labels = customerSpending.map((row) => row.customer);
    const values = customerSpending.map((row) => row.revenue);
    createBarChart("customerSpendingChart", labels, "Revenue", values, "#8fd2b8");
}

function renderForecast(forecastPayload) {
    const historical = forecastPayload.historical || [];
    const forecast = forecastPayload.forecast || [];

    const labels = [...historical.map((x) => x.month), ...forecast.map((x) => x.month)];
    const historicalValues = historical.map((x) => x.revenue);
    const forecastValues = new Array(Math.max(historicalValues.length - 1, 0)).fill(null)
        .concat(historicalValues.length ? [historicalValues[historicalValues.length - 1]] : [])
        .concat(forecast.map((x) => x.revenue));

    destroyChart("forecastChart");
    const ctx = document.getElementById("forecastChart");
    if (!ctx) return;

    chartRegistry.forecastChart = new Chart(ctx, {
        type: "line",
        data: {
            labels,
            datasets: [
                {
                    label: "Historical Revenue",
                    data: historicalValues.concat(new Array(forecast.length).fill(null)),
                    borderColor: "#1f4e8c",
                    backgroundColor: "#1f4e8c22",
                    borderWidth: 2,
                    tension: 0.3,
                    fill: false,
                },
                {
                    label: "Forecast Revenue",
                    data: forecastValues,
                    borderColor: "#2a9d8f",
                    backgroundColor: "#2a9d8f22",
                    borderDash: [6, 6],
                    borderWidth: 2,
                    tension: 0.3,
                    fill: false,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { y: { beginAtZero: true } },
        },
    });
}
