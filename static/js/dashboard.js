const state = {
    analytics: null,
    forecast: null,
    forceDateRangeSync: true,
    lastDateMin: "",
    lastDateMax: "",
};

function getTopProductsFilterOptions() {
    const search = (document.getElementById("topProductsSearch")?.value || "").trim().toLowerCase();
    const limitValue = Number(document.getElementById("topProductsLimit")?.value || 10);
    return { search, limitValue };
}

function updateSalesTrendYearOptions(monthlySales) {
    const yearSelect = document.getElementById("salesTrendYearFilter");
    if (!yearSelect) return;

    const current = yearSelect.value;
    const years = Array.from(
        new Set(
            (monthlySales || [])
                .map((row) => String(row.month || "").slice(0, 4))
                .filter((year) => /^\d{4}$/.test(year))
        )
    ).sort();

    yearSelect.innerHTML = '<option value="">All Years</option>';
    years.forEach((year) => {
        const option = document.createElement("option");
        option.value = year;
        option.textContent = year;
        yearSelect.appendChild(option);
    });

    if (current && years.includes(current)) {
        yearSelect.value = current;
    }
}

function updateSalesTrendChart() {
    const monthlySales = state.analytics?.monthly_sales || [];
    const selectedYear = document.getElementById("salesTrendYearFilter")?.value || "";

    const filtered = selectedYear
        ? monthlySales.filter((row) => String(row.month || "").startsWith(selectedYear))
        : monthlySales;

    renderSalesTrend(filtered);
}

function updateTopProductsChart() {
    const products = state.analytics?.top_products || [];
    const { search, limitValue } = getTopProductsFilterOptions();

    let filtered = products;
    if (search) {
        filtered = filtered.filter((item) => String(item.product || "").toLowerCase().includes(search));
    }

    if (limitValue > 0) {
        filtered = filtered.slice(0, limitValue);
    }

    renderTopProducts(filtered);
}

function currency(value) {
    return new Intl.NumberFormat("en-US", {
        style: "currency",
        currency: "USD",
        maximumFractionDigits: 2,
    }).format(Number(value || 0));
}

function percent(value) {
    return `${Number(value || 0).toFixed(2)}%`;
}

function setStatusMessage(message, kind = "") {
    const status = document.getElementById("uploadStatus");
    if (!status) return;
    status.textContent = message;
    status.className = `status-message ${kind}`.trim();
}

function setLoadingState(isLoading) {
    document.body.classList.toggle("is-loading", isLoading);
}

function renderMappingReport(mappingReport) {
    const panel = document.getElementById("mappingReportPanel");
    const content = document.getElementById("mappingReportContent");
    if (!panel || !content) return;

    if (!mappingReport) {
        panel.hidden = true;
        content.innerHTML = "";
        return;
    }

    const mapped = mappingReport.mapped_columns || {};
    const warnings = mappingReport.warnings || [];
    const mappingRows = Object.entries(mapped)
        .map(([key, value]) => `<li><strong>${key}</strong>: ${value || "(fallback/default)"}</li>`)
        .join("");

    const warningRows = warnings.length
        ? `<ul class="mapping-warnings">${warnings.map((w) => `<li>${w}</li>`).join("")}</ul>`
        : `<p class="mapping-report-empty">No fallback warnings.</p>`;

    content.innerHTML = `<ul class="mapping-list">${mappingRows}</ul>${warningRows}`;
    panel.hidden = false;
}

function getFilters() {
    return {
        date_from: document.getElementById("dateFrom")?.value || "",
        date_to: document.getElementById("dateTo")?.value || "",
        category: document.getElementById("categoryFilter")?.value || "",
        product: document.getElementById("productFilter")?.value || "",
    };
}

function getOptionsFilters() {
    const filters = getFilters();
    // Keep date/category context, but ignore selected product so full product options remain visible.
    filters.product = "";
    return filters;
}

function buildQuery(params) {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
        if (value) query.append(key, value);
    });
    return query.toString() ? `?${query.toString()}` : "";
}

function populateFilterOptions(availableFilters, options = {}) {
    const forceDateRange = Boolean(options.forceDateRange);
    const categorySelect = document.getElementById("categoryFilter");
    const productSelect = document.getElementById("productFilter");
    if (!categorySelect || !productSelect) return;

    const currentCategory = categorySelect.value;
    const currentProduct = productSelect.value;

    categorySelect.innerHTML = '<option value="">All</option>';
    (availableFilters.categories || []).forEach((category) => {
        const option = document.createElement("option");
        option.value = category;
        option.textContent = category;
        categorySelect.appendChild(option);
    });

    productSelect.innerHTML = '<option value="">All</option>';
    (availableFilters.products || []).forEach((product) => {
        const option = document.createElement("option");
        option.value = product;
        option.textContent = product;
        productSelect.appendChild(option);
    });

    if ((availableFilters.categories || []).includes(currentCategory)) {
        categorySelect.value = currentCategory;
    }
    if ((availableFilters.products || []).includes(currentProduct)) {
        productSelect.value = currentProduct;
    }

    const dateFrom = document.getElementById("dateFrom");
    const dateTo = document.getElementById("dateTo");

    const dateMin = availableFilters.date_min || "";
    const dateMax = availableFilters.date_max || "";
    const dateRangeChanged = dateMin !== state.lastDateMin || dateMax !== state.lastDateMax;

    if (dateFrom && dateMin && (forceDateRange || dateRangeChanged || !dateFrom.value)) {
        dateFrom.value = dateMin;
    }
    if (dateTo && dateMax && (forceDateRange || dateRangeChanged || !dateTo.value)) {
        dateTo.value = dateMax;
    }

    state.lastDateMin = dateMin;
    state.lastDateMax = dateMax;
}

function updateMetrics(analytics) {
    document.getElementById("metricRevenue").textContent = currency(analytics.total_revenue);
    document.getElementById("metricOrders").textContent = analytics.total_orders || 0;
    document.getElementById("metricAov").textContent = currency(analytics.average_order_value);

    const growthElement = document.getElementById("metricGrowth");
    const growth = Number(analytics.revenue_growth_pct || 0);
    growthElement.textContent = percent(growth);
    growthElement.classList.toggle("positive", growth > 0);
    growthElement.classList.toggle("negative", growth < 0);

    document.getElementById("metricRepeatRate").textContent = percent(analytics.repeat_customer_rate);
}

function resetDashboardToDefault() {
    const emptyAnalytics = {
        total_revenue: 0,
        total_orders: 0,
        average_order_value: 0,
        revenue_growth_pct: 0,
        repeat_customer_rate: 0,
        summary_statistics: {},
        monthly_sales: [],
        top_products: [],
    };

    state.analytics = emptyAnalytics;
    updateMetrics(emptyAnalytics);
    updateSummaryStats(emptyAnalytics.summary_statistics);
    updateInsights([]);
    renderSalesTrend([]);
    renderTopProducts([]);

    const yearFilter = document.getElementById("salesTrendYearFilter");
    if (yearFilter) {
        yearFilter.innerHTML = '<option value="">All Years</option>';
        yearFilter.value = "";
    }

    const topSearch = document.getElementById("topProductsSearch");
    const topLimit = document.getElementById("topProductsLimit");
    if (topSearch) topSearch.value = "";
    if (topLimit) topLimit.value = "10";

    const panel = document.getElementById("mappingReportPanel");
    const content = document.getElementById("mappingReportContent");
    if (panel) panel.hidden = true;
    if (content) content.innerHTML = "";
}

function updateInsights(insights) {
    const insightsList = document.getElementById("insightsList");
    insightsList.innerHTML = "";

    if (!insights || !insights.length) {
        const li = document.createElement("li");
        li.textContent = "No insights available.";
        insightsList.appendChild(li);
        return;
    }

    insights.forEach((insight) => {
        const li = document.createElement("li");
        li.textContent = insight;
        insightsList.appendChild(li);
    });
}

function updateSummaryStats(summary) {
    const container = document.getElementById("summaryStats");
    if (!container) return;

    const entries = [
        ["Revenue Mean", currency(summary.revenue_mean)],
        ["Revenue Median", currency(summary.revenue_median)],
        ["Revenue Std Dev", currency(summary.revenue_std)],
        ["Average Quantity", Number(summary.quantity_mean || 0).toFixed(2)],
        ["Average Price", currency(summary.price_mean)],
    ];

    container.innerHTML = entries
        .map(([label, value]) => `<div class="stat-row"><span>${label}</span><strong>${value}</strong></div>`)
        .join("");
}

async function fetchJson(url) {
    const response = await fetch(url);
    const payload = await response.json();
    if (!response.ok || payload.success === false) {
        throw new Error(payload.message || "Request failed");
    }
    return payload;
}

async function loadDashboardData() {
    setLoadingState(true);
    const query = buildQuery(getFilters());
    const optionsQuery = buildQuery(getOptionsFilters());

    try {
        const [analyticsPayload, optionsPayload, insightsPayload] = await Promise.all([
            fetchJson(`/get-analytics${query}`),
            fetchJson(`/get-analytics${optionsQuery}`),
            fetchJson(`/get-insights${query}`),
        ]);

        state.analytics = analyticsPayload.analytics;

        updateMetrics(state.analytics);
        populateFilterOptions(optionsPayload.analytics.available_filters || {}, {
            forceDateRange: state.forceDateRangeSync,
        });
        state.forceDateRangeSync = false;
        updateSummaryStats(state.analytics.summary_statistics || {});
        updateInsights(insightsPayload.insights || []);

        updateSalesTrendYearOptions(state.analytics.monthly_sales || []);
        updateSalesTrendChart();
        updateTopProductsChart();
    } finally {
        setLoadingState(false);
    }
}

async function handleUpload(event) {
    event.preventDefault();

    const fileInput = document.getElementById("datasetFile");
    if (!fileInput || !fileInput.files.length) {
        setStatusMessage("Please select a CSV file.", "error");
        return;
    }

    const formData = new FormData();
    formData.append("file", fileInput.files[0]);
    setStatusMessage("Uploading and processing dataset...");

    try {
        const response = await fetch("/upload-dataset", { method: "POST", body: formData });
        const payload = await response.json();

        if (!response.ok || payload.success === false) {
            throw new Error(payload.message || "Upload failed");
        }

        const report = payload.cleaning_report || {};
        setStatusMessage(
            `Upload complete. Rows stored in MySQL: ${payload.stored_rows}. Removed during cleaning: ${report.removed_rows || 0}.`,
            "success"
        );
        renderMappingReport(report.mapping_report || null);
        state.forceDateRangeSync = true;
        await loadDashboardData();
    } catch (error) {
        setStatusMessage(error.message, "error");
    }
}

async function handleResetDataset() {
    const confirmed = window.confirm("Reset dataset and clear all dashboard data?");
    if (!confirmed) return;

    setStatusMessage("Resetting dataset and dashboard...");

    try {
        const response = await fetch("/reset-dataset", { method: "POST" });
        const payload = await response.json();
        if (!response.ok || payload.success === false) {
            throw new Error(payload.message || "Dataset reset failed");
        }

        const fileInput = document.getElementById("datasetFile");
        if (fileInput) fileInput.value = "";

        ["dateFrom", "dateTo", "categoryFilter", "productFilter"].forEach((id) => {
            const element = document.getElementById(id);
            if (element) element.value = "";
        });

        state.forceDateRangeSync = true;
        state.lastDateMin = "";
        state.lastDateMax = "";

        resetDashboardToDefault();
        await loadDashboardData();
        setStatusMessage("Dataset reset complete. Dashboard restored to default state.", "success");
    } catch (error) {
        setStatusMessage(error.message, "error");
    }
}

function resetFilters() {
    ["dateFrom", "dateTo", "categoryFilter", "productFilter"].forEach((id) => {
        const element = document.getElementById(id);
        if (element) element.value = "";
    });
    state.forceDateRangeSync = true;
    loadDashboardData().catch((error) => setStatusMessage(error.message, "error"));
}

function exportAsPdf() {
    window.print();
}

function exportFilteredChartData() {
    const query = buildQuery(getFilters());
    window.location.href = `/export-filtered-chart-data${query}`;
}

document.addEventListener("DOMContentLoaded", () => {
    const uploadForm = document.getElementById("uploadForm");
    if (uploadForm) {
        uploadForm.addEventListener("submit", handleUpload);
    }

    document.getElementById("applyFiltersBtn")?.addEventListener("click", () => {
        loadDashboardData().catch((error) => setStatusMessage(error.message, "error"));
    });

    document.getElementById("resetFiltersBtn")?.addEventListener("click", resetFilters);
    document.getElementById("resetDatasetBtn")?.addEventListener("click", handleResetDataset);
    document.getElementById("exportPdfBtn")?.addEventListener("click", exportAsPdf);
    document.getElementById("exportFilteredBtn")?.addEventListener("click", exportFilteredChartData);
    document.getElementById("salesTrendYearFilter")?.addEventListener("change", updateSalesTrendChart);
    document.getElementById("topProductsSearch")?.addEventListener("input", updateTopProductsChart);
    document.getElementById("topProductsLimit")?.addEventListener("change", updateTopProductsChart);

    loadDashboardData().catch((error) => {
        setStatusMessage(`Waiting for dataset upload: ${error.message}`, "error");
    });
});
