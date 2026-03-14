import { useEffect, useMemo, useState } from "react";
import "./App.css";
import { API_BASE_URL, apiRequest, getStoredToken, setStoredToken } from "./api";

const emptySummary = {
  totals: {
    farmers: 0,
    active_crops: 0,
    cost: 0,
    revenue: 0,
    profit: 0,
  },
  stage_distribution: [],
  upcoming_harvests: [],
};

const emptyFpoSummary = {
  portfolio_totals: {
    farmers: 0,
    profitable_farmers: 0,
    revenue_generating_farmers: 0,
  },
  top_revenue_farmers: [],
  attention_required: [],
};

function formatCurrency(value) {
  return new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
    maximumFractionDigits: 0,
  }).format(value || 0);
}

function App() {
  const [token, setToken] = useState(() => getStoredToken());
  const [user, setUser] = useState(null);
  const [authLoading, setAuthLoading] = useState(Boolean(getStoredToken()));
  const [authError, setAuthError] = useState("");

  useEffect(() => {
    let cancelled = false;

    async function hydrateSession() {
      if (!token) {
        setAuthLoading(false);
        setUser(null);
        return;
      }

      setAuthLoading(true);
      try {
        const response = await apiRequest("/auth/me", { token });
        if (!cancelled) {
          setUser(response.user);
          setAuthError("");
        }
      } catch (error) {
        if (!cancelled) {
          handleLogout();
          setAuthError("Your session expired. Please sign in again.");
        }
      } finally {
        if (!cancelled) {
          setAuthLoading(false);
        }
      }
    }

    hydrateSession();

    return () => {
      cancelled = true;
    };
  }, [token]);

  function handleLoginSuccess(payload) {
    setStoredToken(payload.access_token);
    setToken(payload.access_token);
    setUser(payload.user);
    setAuthError("");
  }

  function handleLogout() {
    setStoredToken("");
    setToken("");
    setUser(null);
  }

  if (authLoading) {
    return <LoadingScreen title="Restoring your Prithvi session..." />;
  }

  if (!user) {
    return (
      <AuthScreen
        authError={authError}
        onLoginSuccess={handleLoginSuccess}
      />
    );
  }

  return (
    <ProtectedShell user={user} onLogout={handleLogout}>
      {user.role === "admin" ? (
        <AdminDashboard token={token} user={user} />
      ) : (
        <FarmerDashboard token={token} user={user} />
      )}
    </ProtectedShell>
  );
}

function AuthScreen({ authError, onLoginSuccess }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState(authError || "");

  useEffect(() => {
    setError(authError || "");
  }, [authError]);

  async function handleSubmit(event) {
    event.preventDefault();
    setSubmitting(true);
    setError("");

    try {
      const payload = await apiRequest("/auth/login", {
        method: "POST",
        body: { username, password },
      });
      onLoginSuccess(payload);
    } catch (requestError) {
      setError(requestError.message || "Unable to sign in.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="shell auth-shell">
      <div className="backdrop" />

      <section className="auth-panel">
        <div className="auth-copy">
          <p className="eyebrow">Prithvi Secure Console</p>
          <h1>Sign in to your farm operating system</h1>
          <p>
            Use your Prithvi credentials to access the protected portfolio dashboard,
            farmer ledger, crop economics, and sales workflows.
          </p>

          <div className="meta-stack">
            <div className="meta-pill">
              <span>API</span>
              <strong>{API_BASE_URL.replace(/^https?:\/\//, "")}</strong>
            </div>
            <div className="meta-pill">
              <span>Security</span>
              <strong>Bearer token session</strong>
            </div>
          </div>
        </div>

        <form className="login-card" onSubmit={handleSubmit}>
          <p className="panel-kicker">Login</p>
          <h2>Welcome back</h2>

          <label className="field">
            <span>Username</span>
            <input
              value={username}
              onChange={(event) => setUsername(event.target.value)}
              placeholder="Enter your username"
              autoComplete="username"
              required
            />
          </label>

          <label className="field">
            <span>Password</span>
            <input
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              placeholder="Enter your password"
              type="password"
              autoComplete="current-password"
              required
            />
          </label>

          {error ? <div className="inline-message error">{error}</div> : null}

          <button className="primary-button" disabled={submitting} type="submit">
            {submitting ? "Signing in..." : "Sign in"}
          </button>
        </form>
      </section>
    </div>
  );
}

function ProtectedShell({ user, onLogout, children }) {
  return (
    <div className="shell">
      <div className="backdrop" />

      <header className="hero">
        <div>
          <p className="eyebrow">Prithvi</p>
          <h1>Agricultural Operating System</h1>
          <p className="hero-copy">
            Protected workspace for crop economics, farmer operations, and post-harvest sales.
          </p>
        </div>

        <div className="hero-meta">
          <div className="meta-pill">
            <span>Signed in as</span>
            <strong>{user.username}</strong>
          </div>
          <div className="meta-pill">
            <span>Role</span>
            <strong className="role-text">{user.role}</strong>
          </div>
          <button className="secondary-button" onClick={onLogout} type="button">
            Logout
          </button>
        </div>
      </header>

      {children}
    </div>
  );
}

function AdminDashboard({ token, user }) {
  const [farmers, setFarmers] = useState([]);
  const [summary, setSummary] = useState(emptySummary);
  const [alerts, setAlerts] = useState({ upcoming_harvests: [] });
  const [fpoSummary, setFpoSummary] = useState(emptyFpoSummary);
  const [selectedFarmer, setSelectedFarmer] = useState("");
  const [farmerLedger, setFarmerLedger] = useState(null);
  const [ledgerLoading, setLedgerLoading] = useState(false);
  const [ledgerError, setLedgerError] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [lastUpdated, setLastUpdated] = useState("");
  const [ledgerRefreshKey, setLedgerRefreshKey] = useState(0);
  const [dashboardRefreshKey, setDashboardRefreshKey] = useState(0);

  useEffect(() => {
    let cancelled = false;

    async function loadAdminData() {
      setLoading(true);
      setError("");

      const results = await Promise.allSettled([
        apiRequest("/farmers", { token }),
        apiRequest("/dashboard/summary", { token }),
        apiRequest("/alerts/overview", { token }),
        apiRequest("/dashboard/fpo-summary", { token }),
      ]);

      if (cancelled) {
        return;
      }

      const [farmersResult, summaryResult, alertsResult, fpoResult] = results;

      const farmerRows = farmersResult.status === "fulfilled" ? (farmersResult.value.farmers || []) : [];
      setFarmers(farmerRows);
      if (farmerRows.length > 0) {
        setSelectedFarmer((current) => current || farmerRows[0].name);
      }
      setSummary(summaryResult.status === "fulfilled" ? { ...emptySummary, ...summaryResult.value } : emptySummary);
      setAlerts(alertsResult.status === "fulfilled" ? (alertsResult.value || { upcoming_harvests: [] }) : { upcoming_harvests: [] });
      setFpoSummary(fpoResult.status === "fulfilled" ? { ...emptyFpoSummary, ...fpoResult.value } : emptyFpoSummary);

      if (results.some((result) => result.status === "rejected")) {
        setError("Some admin data could not be loaded. The dashboard is showing the data that succeeded.");
      }

      setLastUpdated(new Date().toLocaleString("en-IN"));
      setLoading(false);
    }

    loadAdminData();

    return () => {
      cancelled = true;
    };
  }, [token, dashboardRefreshKey]);

  useEffect(() => {
    let cancelled = false;

    async function loadFarmerLedger() {
      if (!selectedFarmer) {
        setFarmerLedger(null);
        return;
      }

      setLedgerLoading(true);
      setLedgerError("");

      try {
        const response = await apiRequest(`/farmer/${encodeURIComponent(selectedFarmer)}/full-ledger`, { token });
        if (!cancelled) {
          setFarmerLedger(response);
        }
      } catch (requestError) {
        if (!cancelled) {
          setFarmerLedger(null);
          setLedgerError(requestError.message || "Unable to load the selected farmer ledger.");
        }
      } finally {
        if (!cancelled) {
          setLedgerLoading(false);
        }
      }
    }

    loadFarmerLedger();

    return () => {
      cancelled = true;
    };
  }, [selectedFarmer, token, ledgerRefreshKey]);

  function refreshSelectedLedger() {
    setLedgerRefreshKey((value) => value + 1);
  }

  function refreshAdminData(options = {}) {
    setDashboardRefreshKey((value) => value + 1);
    if (options.refreshLedger) {
      refreshSelectedLedger();
    }
  }

  const cards = useMemo(() => ([
    { label: "Total Farmers", value: summary.totals.farmers, tone: "forest" },
    { label: "Active Crops", value: summary.totals.active_crops, tone: "leaf" },
    { label: "Total Cost", value: formatCurrency(summary.totals.cost), tone: "amber" },
    { label: "Total Revenue", value: formatCurrency(summary.totals.revenue), tone: "river" },
  ]), [summary]);

  if (loading) {
    return <LoadingScreen title="Loading admin dashboard..." />;
  }

  return (
    <>
      <section className={`banner ${error ? "warning" : "success"}`}>
        <strong>{error ? "Partial admin data loaded." : `Admin workspace ready for ${user.username}.`}</strong>
        <span>{error || `Last updated ${lastUpdated || "just now"}. Portfolio visibility is live.`}</span>
      </section>

      <section className="card-grid">
        {cards.map((card) => <MetricCard key={card.label} {...card} />)}
      </section>

      <section className="content-grid">
        <Panel
          kicker="Operations Watch"
          title="Upcoming Harvests"
          count={alerts.upcoming_harvests?.length || 0}
        >
          {alerts.upcoming_harvests?.length ? (
            <div className="alert-list">
              {alerts.upcoming_harvests.map((item) => (
                <div className="alert-item" key={`${item.crop_id}-${item.expected_harvest}`}>
                  <div>
                    <strong>{item.farmer}</strong>
                    <p>{item.crop}</p>
                  </div>
                  <div className="alert-meta">
                    <span>{item.days_to_harvest} day(s)</span>
                    <small>{item.expected_harvest}</small>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <EmptyState title="No urgent harvests" body="The API did not return harvests inside the alert window." />
          )}
        </Panel>

        <Panel kicker="Portfolio Shape" title="Stage Distribution">
          {summary.stage_distribution?.length ? (
            <div className="stage-stack">
              {summary.stage_distribution.map((stage) => (
                <div className="stage-row" key={stage.stage}>
                  <div>
                    <strong>{stage.stage}</strong>
                    <p>{stage.crop_count} crop(s)</p>
                  </div>
                  <div className="stage-bar" style={{ width: `${Math.max(16, stage.crop_count * 20)}px` }} />
                </div>
              ))}
            </div>
          ) : (
            <EmptyState title="No stage data yet" body="Stage distribution appears here once the backend returns portfolio breakdown data." />
          )}
        </Panel>
      </section>

      <section className="content-grid">
        <Panel kicker="Revenue Leaders" title="Top Farmers" count={fpoSummary.top_revenue_farmers.length}>
          {fpoSummary.top_revenue_farmers.length ? (
            <div className="leader-list">
              {fpoSummary.top_revenue_farmers.map((farmer) => (
                <div className="leader-item" key={farmer.farmer_id}>
                  <div>
                    <strong>{farmer.name}</strong>
                    <p>{farmer.village}</p>
                  </div>
                  <div className="leader-metrics">
                    <span>{formatCurrency(farmer.total_revenue)}</span>
                    <small>{farmer.crop_count} crop(s)</small>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <EmptyState title="No revenue leaders yet" body="Top revenue farmers will appear here once deals and harvests are recorded." />
          )}
        </Panel>

        <Panel kicker="Attention Queue" title="Action Required" count={fpoSummary.attention_required.length}>
          {fpoSummary.attention_required.length ? (
            <div className="leader-list">
              {fpoSummary.attention_required.map((item) => (
                <div className="leader-item" key={item.crop_id}>
                  <div>
                    <strong>{item.farmer}</strong>
                    <p>{item.crop} · {item.stage}</p>
                  </div>
                  <div className="leader-metrics">
                    <span>{formatCurrency(item.total_cost)}</span>
                    <small>{item.expected_harvest || "No harvest date"}</small>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <EmptyState title="No open attention items" body="Operational flags from the FPO summary will appear here." />
          )}
        </Panel>
      </section>

      <section className="content-grid">
        <AdminFarmerCreatePanel token={token} onCreated={refreshAdminData} />
        <AdminCropCreatePanel
          token={token}
          farmers={farmers}
          selectedFarmer={selectedFarmer}
          onCreated={() => refreshAdminData({ refreshLedger: true })}
        />
      </section>

      <Panel kicker="Farmer Registry" title="Farmer Portfolio" count={farmers.length} className="table-panel">
        {farmers.length ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Name</th>
                  <th>Village</th>
                  <th>Land (acres)</th>
                </tr>
              </thead>
              <tbody>
                {farmers.map((farmer) => (
                  <tr
                    key={farmer.id}
                    className={selectedFarmer === farmer.name ? "selected-row" : ""}
                    onClick={() => setSelectedFarmer(farmer.name)}
                  >
                    <td>{farmer.id}</td>
                    <td>{farmer.name}</td>
                    <td>{farmer.village}</td>
                    <td>{farmer.land_acres}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <EmptyState title="No farmer rows loaded" body="The API did not return farmer records for this admin session." />
        )}
      </Panel>

      <Panel
        kicker="Admin Drill-down"
        title={selectedFarmer ? `${selectedFarmer} · Full Ledger` : "Farmer Detail"}
        className="table-panel"
      >
        {ledgerLoading ? (
          <div className="empty-state">
            <strong>Loading farmer ledger...</strong>
            <p>Fetching full details for the selected farmer.</p>
          </div>
        ) : ledgerError ? (
          <EmptyState title="Unable to load selected farmer" body={ledgerError} />
        ) : farmerLedger ? (
          <AdminFarmerDetail ledger={farmerLedger} token={token} onRefresh={refreshSelectedLedger} />
        ) : (
          <EmptyState title="No farmer selected" body="Choose a farmer from the table above to inspect the full ledger." />
        )}
      </Panel>
    </>
  );
}

function FarmerDashboard({ token, user }) {
  const [ledger, setLedger] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [refreshKey, setRefreshKey] = useState(0);

  useEffect(() => {
    let cancelled = false;

    async function loadFarmerData() {
      setLoading(true);
      setError("");

      try {
        const response = await apiRequest("/farmer/me/full-ledger", { token });
        if (!cancelled) {
          setLedger(response);
        }
      } catch (requestError) {
        if (!cancelled) {
          setLedger(null);
          setError(requestError.message || "Unable to load your farmer ledger.");
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    loadFarmerData();

    return () => {
      cancelled = true;
    };
  }, [token, refreshKey]);

  function refreshLedger() {
    setRefreshKey((value) => value + 1);
  }

  if (loading) {
    return <LoadingScreen title="Loading your farmer ledger..." />;
  }

  if (!ledger) {
    return (
      <section className="banner warning">
        <strong>Farmer workspace unavailable.</strong>
        <span>{error || "Your linked farmer ledger could not be loaded from the API."}</span>
      </section>
    );
  }

  const farmer = ledger.farmer;
  const economics = ledger.economics;

  return (
    <>
      <section className="banner success">
        <strong>Farmer workspace ready for {user.username}.</strong>
        <span>You are viewing the protected ledger linked to farmer ID {user.farmer_id}.</span>
      </section>

      <section className="content-grid">
        <Panel kicker="Farmer Profile" title={farmer.name}>
          <div className="profile-grid">
            <InfoPair label="Village" value={farmer.village} />
            <InfoPair label="District" value={farmer.district} />
            <InfoPair label="State" value={farmer.state} />
            <InfoPair label="Land" value={`${farmer.land_acres} acres`} />
            <InfoPair label="Phone" value={farmer.phone || "Not available"} />
          </div>
        </Panel>

        <Panel kicker="Economics" title="Ledger Snapshot">
          <div className="profile-grid">
            <InfoPair label="Total Cost" value={formatCurrency(economics.total_cost)} />
            <InfoPair label="Gross Sales" value={formatCurrency(economics.gross_sales || 0)} />
            <InfoPair label="Amount Received" value={formatCurrency(economics.amount_received || 0)} />
            <InfoPair label="Outstanding" value={formatCurrency(economics.outstanding_amount || 0)} />
            <InfoPair label="Profit" value={formatCurrency(economics.profit || 0)} />
            <InfoPair label="Realized Profit" value={formatCurrency(economics.realized_profit || 0)} />
          </div>
        </Panel>
      </section>

        <Panel kicker="Crop Ledger" title="Your Crops" count={ledger.crops.length}>
        {ledger.crops.length ? (
          <div className="crop-stack">
            {ledger.crops.map((crop) => (
              <CropLedgerCard
                key={crop.crop_id}
                crop={crop}
                token={token}
                canManageDeals
                onRefresh={refreshLedger}
              />
            ))}
          </div>
        ) : (
          <EmptyState title="No crop ledger yet" body="This farmer account does not have any crop records yet." />
        )}
      </Panel>
    </>
  );
}

function AdminFarmerDetail({ ledger, token, onRefresh }) {
  const farmer = ledger.farmer;
  const economics = ledger.economics;

  return (
    <div className="drilldown-stack">
      <section className="content-grid">
        <Panel kicker="Farmer Profile" title={farmer.name}>
          <div className="profile-grid">
            <InfoPair label="Village" value={farmer.village} />
            <InfoPair label="District" value={farmer.district} />
            <InfoPair label="State" value={farmer.state} />
            <InfoPair label="Land" value={`${farmer.land_acres} acres`} />
            <InfoPair label="Phone" value={farmer.phone || "Not available"} />
          </div>
        </Panel>

        <Panel kicker="Economics" title="Farmer Ledger Snapshot">
          <div className="profile-grid">
            <InfoPair label="Total Cost" value={formatCurrency(economics.total_cost)} />
            <InfoPair label="Gross Sales" value={formatCurrency(economics.gross_sales || 0)} />
            <InfoPair label="Amount Received" value={formatCurrency(economics.amount_received || 0)} />
            <InfoPair label="Outstanding" value={formatCurrency(economics.outstanding_amount || 0)} />
            <InfoPair label="Profit" value={formatCurrency(economics.profit || 0)} />
            <InfoPair label="Realized Profit" value={formatCurrency(economics.realized_profit || 0)} />
          </div>
        </Panel>
      </section>

      <div className="crop-stack">
        {ledger.crops?.length ? (
          ledger.crops.map((crop) => (
            <CropLedgerCard
              key={crop.crop_id}
              crop={crop}
              token={token}
              canManageDeals
              onRefresh={onRefresh}
            />
          ))
        ) : (
          <EmptyState title="No crops in this ledger" body="The selected farmer does not have crop records yet." />
        )}
      </div>
    </div>
  );
}

function AdminFarmerCreatePanel({ token, onCreated }) {
  const [form, setForm] = useState({
    name: "",
    phone: "",
    village: "",
    district: "",
    state: "",
    land_acres: "",
  });
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");

  async function handleSubmit(event) {
    event.preventDefault();
    setSubmitting(true);
    setError("");
    setSuccess("");

    try {
      await apiRequest("/farmers/add", {
        method: "POST",
        token,
        body: {
          ...form,
          land_acres: Number(form.land_acres),
        },
      });
      setForm({
        name: "",
        phone: "",
        village: "",
        district: "",
        state: "",
        land_acres: "",
      });
      setSuccess("Farmer created successfully.");
      onCreated();
    } catch (requestError) {
      setError(requestError.message || "Unable to create farmer.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Panel kicker="Create" title="Add Farmer">
      <form className="deal-form-grid" onSubmit={handleSubmit}>
        <label className="field">
          <span>Name</span>
          <input value={form.name} onChange={(event) => setForm((state) => ({ ...state, name: event.target.value }))} required />
        </label>
        <label className="field">
          <span>Phone</span>
          <input value={form.phone} onChange={(event) => setForm((state) => ({ ...state, phone: event.target.value }))} required />
        </label>
        <label className="field">
          <span>Village</span>
          <input value={form.village} onChange={(event) => setForm((state) => ({ ...state, village: event.target.value }))} required />
        </label>
        <label className="field">
          <span>District</span>
          <input value={form.district} onChange={(event) => setForm((state) => ({ ...state, district: event.target.value }))} required />
        </label>
        <label className="field">
          <span>State</span>
          <input value={form.state} onChange={(event) => setForm((state) => ({ ...state, state: event.target.value }))} required />
        </label>
        <label className="field">
          <span>Land (acres)</span>
          <input
            type="number"
            min="0"
            step="0.01"
            value={form.land_acres}
            onChange={(event) => setForm((state) => ({ ...state, land_acres: event.target.value }))}
            required
          />
        </label>

        {error ? <div className="inline-message error field-wide">{error}</div> : null}
        {success ? <div className="inline-message success field-wide">{success}</div> : null}

        <div className="field-wide form-actions">
          <button className="primary-button" disabled={submitting} type="submit">
            {submitting ? "Creating..." : "Create farmer"}
          </button>
        </div>
      </form>
    </Panel>
  );
}

function AdminCropCreatePanel({ token, farmers, selectedFarmer, onCreated }) {
  const selectedFarmerRecord = farmers.find((farmer) => farmer.name === selectedFarmer);
  const [form, setForm] = useState({
    farmer_id: "",
    crop_type: "",
    variety: "",
    season: "rabi",
    year: new Date().getFullYear(),
    area_acres: "",
    sowing_date: "",
    expected_harvest: "",
    expected_yield_quintal: "",
  });
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");

  useEffect(() => {
    if (selectedFarmerRecord) {
      setForm((state) => ({ ...state, farmer_id: String(selectedFarmerRecord.id) }));
    }
  }, [selectedFarmerRecord]);

  async function handleSubmit(event) {
    event.preventDefault();
    setSubmitting(true);
    setError("");
    setSuccess("");

    try {
      await apiRequest("/crops/add", {
        method: "POST",
        token,
        body: {
          ...form,
          farmer_id: Number(form.farmer_id),
          year: Number(form.year),
          area_acres: Number(form.area_acres),
          expected_yield_quintal: Number(form.expected_yield_quintal),
        },
      });
      setForm((state) => ({
        ...state,
        crop_type: "",
        variety: "",
        area_acres: "",
        sowing_date: "",
        expected_harvest: "",
        expected_yield_quintal: "",
      }));
      setSuccess("Crop created successfully.");
      onCreated();
    } catch (requestError) {
      setError(requestError.message || "Unable to create crop.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Panel kicker="Create" title="Add Crop">
      <form className="deal-form-grid" onSubmit={handleSubmit}>
        <label className="field">
          <span>Farmer</span>
          <select
            value={form.farmer_id}
            onChange={(event) => setForm((state) => ({ ...state, farmer_id: event.target.value }))}
            required
          >
            <option value="">Select farmer</option>
            {farmers.map((farmer) => (
              <option key={farmer.id} value={farmer.id}>{farmer.name}</option>
            ))}
          </select>
        </label>
        <label className="field">
          <span>Crop Type</span>
          <input value={form.crop_type} onChange={(event) => setForm((state) => ({ ...state, crop_type: event.target.value }))} required />
        </label>
        <label className="field">
          <span>Variety</span>
          <input value={form.variety} onChange={(event) => setForm((state) => ({ ...state, variety: event.target.value }))} required />
        </label>
        <label className="field">
          <span>Season</span>
          <select value={form.season} onChange={(event) => setForm((state) => ({ ...state, season: event.target.value }))}>
            <option value="kharif">Kharif</option>
            <option value="rabi">Rabi</option>
            <option value="zaid">Zaid</option>
          </select>
        </label>
        <label className="field">
          <span>Year</span>
          <input type="number" value={form.year} onChange={(event) => setForm((state) => ({ ...state, year: event.target.value }))} required />
        </label>
        <label className="field">
          <span>Area (acres)</span>
          <input
            type="number"
            min="0"
            step="0.01"
            value={form.area_acres}
            onChange={(event) => setForm((state) => ({ ...state, area_acres: event.target.value }))}
            required
          />
        </label>
        <label className="field">
          <span>Sowing Date</span>
          <input type="date" value={form.sowing_date} onChange={(event) => setForm((state) => ({ ...state, sowing_date: event.target.value }))} required />
        </label>
        <label className="field">
          <span>Expected Harvest</span>
          <input type="date" value={form.expected_harvest} onChange={(event) => setForm((state) => ({ ...state, expected_harvest: event.target.value }))} required />
        </label>
        <label className="field">
          <span>Expected Yield (quintal)</span>
          <input
            type="number"
            min="0"
            step="0.01"
            value={form.expected_yield_quintal}
            onChange={(event) => setForm((state) => ({ ...state, expected_yield_quintal: event.target.value }))}
            required
          />
        </label>

        {error ? <div className="inline-message error field-wide">{error}</div> : null}
        {success ? <div className="inline-message success field-wide">{success}</div> : null}

        <div className="field-wide form-actions">
          <button className="primary-button" disabled={submitting} type="submit">
            {submitting ? "Creating..." : "Create crop"}
          </button>
        </div>
      </form>
    </Panel>
  );
}

function CropLedgerCard({ crop, token, canManageDeals, onRefresh }) {
  const [costForm, setCostForm] = useState({
    stage: crop.stage || "sowing",
    item_name: "",
    quantity: "",
    unit: "",
    amount: "",
  });
  const [harvestForm, setHarvestForm] = useState({
    harvest_date: "",
    yield_quintal: "",
    selling_price: "",
    buyer: "",
  });
  const [dealForm, setDealForm] = useState({
    sale_date: "",
    quantity_quintal: "",
    price_per_quintal: "",
    buyer: "",
    amount_received: "",
    notes: "",
  });
  const [paymentDrafts, setPaymentDrafts] = useState({});
  const [submittingCost, setSubmittingCost] = useState(false);
  const [submittingHarvest, setSubmittingHarvest] = useState(false);
  const [submittingDeal, setSubmittingDeal] = useState(false);
  const [dealError, setDealError] = useState("");
  const [dealSuccess, setDealSuccess] = useState("");

  async function handleCostSubmit(event) {
    event.preventDefault();
    setSubmittingCost(true);
    setDealError("");
    setDealSuccess("");

    try {
      await apiRequest("/costs/add", {
        method: "POST",
        token,
        body: {
          crop_id: crop.crop_id,
          stage: costForm.stage,
          item_name: costForm.item_name,
          quantity: Number(costForm.quantity),
          unit: costForm.unit,
          amount: Number(costForm.amount),
        },
      });
      setCostForm({
        stage: crop.stage || "sowing",
        item_name: "",
        quantity: "",
        unit: "",
        amount: "",
      });
      setDealSuccess("Cost logged successfully.");
      onRefresh();
    } catch (error) {
      setDealError(error.message || "Unable to log cost.");
    } finally {
      setSubmittingCost(false);
    }
  }

  async function handleHarvestSubmit(event) {
    event.preventDefault();
    setSubmittingHarvest(true);
    setDealError("");
    setDealSuccess("");

    try {
      await apiRequest("/harvests/add", {
        method: "POST",
        token,
        body: {
          crop_id: crop.crop_id,
          harvest_date: harvestForm.harvest_date,
          yield_quintal: Number(harvestForm.yield_quintal),
          selling_price: Number(harvestForm.selling_price),
          buyer: harvestForm.buyer,
        },
      });
      setHarvestForm({
        harvest_date: "",
        yield_quintal: "",
        selling_price: "",
        buyer: "",
      });
      setDealSuccess("Harvest logged successfully.");
      onRefresh();
    } catch (error) {
      setDealError(error.message || "Unable to log harvest.");
    } finally {
      setSubmittingHarvest(false);
    }
  }

  async function handleDealSubmit(event) {
    event.preventDefault();
    setSubmittingDeal(true);
    setDealError("");
    setDealSuccess("");

    try {
      await apiRequest("/deals/add", {
        method: "POST",
        token,
        body: {
          crop_id: crop.crop_id,
          sale_date: dealForm.sale_date,
          quantity_quintal: Number(dealForm.quantity_quintal),
          price_per_quintal: Number(dealForm.price_per_quintal),
          buyer: dealForm.buyer,
          amount_received: Number(dealForm.amount_received || 0),
          notes: dealForm.notes || null,
        },
      });

      setDealForm({
        sale_date: "",
        quantity_quintal: "",
        price_per_quintal: "",
        buyer: "",
        amount_received: "",
        notes: "",
      });
      setDealSuccess("Deal recorded successfully.");
      onRefresh();
    } catch (error) {
      setDealError(error.message || "Unable to record deal.");
    } finally {
      setSubmittingDeal(false);
    }
  }

  async function handlePaymentUpdate(dealId) {
    const draft = paymentDrafts[dealId];
    if (draft === undefined || draft === "") {
      return;
    }

    setDealError("");
    setDealSuccess("");

    try {
      await apiRequest(`/deals/${dealId}/payment`, {
        method: "PATCH",
        token,
        body: { amount_received: Number(draft) },
      });
      setDealSuccess("Payment updated successfully.");
      onRefresh();
    } catch (error) {
      setDealError(error.message || "Unable to update payment.");
    }
  }

  return (
    <article className="crop-card">
      <div className="crop-head">
        <div>
          <h3>{crop.crop}</h3>
          <p>{crop.variety} · {crop.season} {crop.year}</p>
        </div>
        <span className="status-pill">{crop.stage}</span>
      </div>

      <div className="profile-grid compact">
        <InfoPair label="Expected Harvest" value={crop.expected_harvest} />
        <InfoPair label="Expected Yield" value={`${crop.expected_yield_quintal ?? 0} quintal`} />
        <InfoPair label="Total Cost" value={formatCurrency(crop.total_cost)} />
        <InfoPair label="Gross Sales" value={formatCurrency(crop.gross_sales || crop.total_revenue || 0)} />
        <InfoPair label="Amount Received" value={formatCurrency(crop.amount_received || 0)} />
        <InfoPair
          label="Outstanding"
          value={formatCurrency(Math.max(0, (crop.gross_sales || crop.total_revenue || 0) - (crop.amount_received || 0)))}
        />
      </div>

      <div className="mini-grid">
        <MiniList
          title="Costs"
          items={crop.costs?.map((item) => `${item.stage}: ${item.item_name} (${formatCurrency(item.amount)})`) || []}
          emptyText="No cost entries yet"
        />
        <DealList
          crop={crop}
          paymentDrafts={paymentDrafts}
          setPaymentDrafts={setPaymentDrafts}
          onPaymentUpdate={handlePaymentUpdate}
        />
      </div>

      {canManageDeals ? (
        <div className="deal-form-shell">
          <div className="deal-form-header">
            <div>
              <p className="panel-kicker">Operations Workflow</p>
              <h4>Log a cost</h4>
            </div>
          </div>

          <form className="deal-form-grid" onSubmit={handleCostSubmit}>
            <label className="field">
              <span>Stage</span>
              <select
                value={costForm.stage}
                onChange={(event) => setCostForm((state) => ({ ...state, stage: event.target.value }))}
              >
                <option value="sowing">Sowing</option>
                <option value="growing">Growing</option>
                <option value="harvest">Harvest</option>
                <option value="logistics">Logistics</option>
                <option value="storage">Storage</option>
              </select>
            </label>

            <label className="field">
              <span>Item Name</span>
              <input
                value={costForm.item_name}
                onChange={(event) => setCostForm((state) => ({ ...state, item_name: event.target.value }))}
                required
              />
            </label>

            <label className="field">
              <span>Quantity</span>
              <input
                type="number"
                min="0"
                step="0.01"
                value={costForm.quantity}
                onChange={(event) => setCostForm((state) => ({ ...state, quantity: event.target.value }))}
                required
              />
            </label>

            <label className="field">
              <span>Unit</span>
              <input
                value={costForm.unit}
                onChange={(event) => setCostForm((state) => ({ ...state, unit: event.target.value }))}
                required
              />
            </label>

            <label className="field">
              <span>Amount</span>
              <input
                type="number"
                min="0"
                step="0.01"
                value={costForm.amount}
                onChange={(event) => setCostForm((state) => ({ ...state, amount: event.target.value }))}
                required
              />
            </label>

            <div className="field-wide form-actions">
              <button className="primary-button" disabled={submittingCost} type="submit">
                {submittingCost ? "Logging..." : "Add cost"}
              </button>
            </div>
          </form>
        </div>
      ) : null}

      {canManageDeals ? (
        <div className="deal-form-shell">
          <div className="deal-form-header">
            <div>
              <p className="panel-kicker">Harvest Workflow</p>
              <h4>Log harvest</h4>
            </div>
          </div>

          <form className="deal-form-grid" onSubmit={handleHarvestSubmit}>
            <label className="field">
              <span>Harvest Date</span>
              <input
                type="date"
                value={harvestForm.harvest_date}
                onChange={(event) => setHarvestForm((state) => ({ ...state, harvest_date: event.target.value }))}
                required
              />
            </label>

            <label className="field">
              <span>Yield (quintal)</span>
              <input
                type="number"
                min="0"
                step="0.01"
                value={harvestForm.yield_quintal}
                onChange={(event) => setHarvestForm((state) => ({ ...state, yield_quintal: event.target.value }))}
                required
              />
            </label>

            <label className="field">
              <span>Selling Price</span>
              <input
                type="number"
                min="0"
                step="0.01"
                value={harvestForm.selling_price}
                onChange={(event) => setHarvestForm((state) => ({ ...state, selling_price: event.target.value }))}
                required
              />
            </label>

            <label className="field">
              <span>Buyer</span>
              <input
                value={harvestForm.buyer}
                onChange={(event) => setHarvestForm((state) => ({ ...state, buyer: event.target.value }))}
                required
              />
            </label>

            <div className="field-wide form-actions">
              <button className="primary-button" disabled={submittingHarvest} type="submit">
                {submittingHarvest ? "Logging..." : "Add harvest"}
              </button>
            </div>
          </form>
        </div>
      ) : null}

      {canManageDeals ? (
        <div className="deal-form-shell">
          <div className="deal-form-header">
            <div>
              <p className="panel-kicker">Sales Workflow</p>
              <h4>Record a new deal</h4>
            </div>
          </div>

          <form className="deal-form-grid" onSubmit={handleDealSubmit}>
            <label className="field">
              <span>Sale Date</span>
              <input
                type="date"
                value={dealForm.sale_date}
                onChange={(event) => setDealForm((state) => ({ ...state, sale_date: event.target.value }))}
                required
              />
            </label>

            <label className="field">
              <span>Quantity (quintal)</span>
              <input
                type="number"
                min="0"
                step="0.01"
                value={dealForm.quantity_quintal}
                onChange={(event) => setDealForm((state) => ({ ...state, quantity_quintal: event.target.value }))}
                required
              />
            </label>

            <label className="field">
              <span>Price / quintal</span>
              <input
                type="number"
                min="0"
                step="0.01"
                value={dealForm.price_per_quintal}
                onChange={(event) => setDealForm((state) => ({ ...state, price_per_quintal: event.target.value }))}
                required
              />
            </label>

            <label className="field">
              <span>Buyer</span>
              <input
                value={dealForm.buyer}
                onChange={(event) => setDealForm((state) => ({ ...state, buyer: event.target.value }))}
                required
              />
            </label>

            <label className="field">
              <span>Amount Received</span>
              <input
                type="number"
                min="0"
                step="0.01"
                value={dealForm.amount_received}
                onChange={(event) => setDealForm((state) => ({ ...state, amount_received: event.target.value }))}
              />
            </label>

            <label className="field field-wide">
              <span>Notes</span>
              <input
                value={dealForm.notes}
                onChange={(event) => setDealForm((state) => ({ ...state, notes: event.target.value }))}
              />
            </label>

            <div className="field-wide form-actions">
              <button className="primary-button" disabled={submittingDeal} type="submit">
                {submittingDeal ? "Recording..." : "Add deal"}
              </button>
            </div>
          </form>
        </div>
      ) : null}

      {dealError ? <div className="inline-message error">{dealError}</div> : null}
      {dealSuccess ? <div className="inline-message success">{dealSuccess}</div> : null}
    </article>
  );
}

function DealList({ crop, paymentDrafts, setPaymentDrafts, onPaymentUpdate }) {
  const deals = crop.deals || [];

  if (!deals.length) {
    return <MiniList title="Deals" items={[]} emptyText="No sale deals yet" />;
  }

  return (
    <div className="mini-list">
      <h4>Deals</h4>
      <div className="deal-stack">
        {deals.map((deal) => (
          <div className="deal-item" key={deal.deal_id}>
            <div className="deal-line">
              <strong>{deal.buyer}</strong>
              <span className={`payment-pill ${deal.payment_status}`}>{deal.payment_status}</span>
            </div>
            <p>{formatCurrency(deal.gross_amount)} · received {formatCurrency(deal.amount_received)}</p>
            <div className="payment-editor">
              <input
                type="number"
                min="0"
                step="0.01"
                value={paymentDrafts[deal.deal_id] ?? deal.amount_received}
                onChange={(event) => setPaymentDrafts((state) => ({
                  ...state,
                  [deal.deal_id]: event.target.value,
                }))}
              />
              <button className="secondary-inline-button" onClick={() => onPaymentUpdate(deal.deal_id)} type="button">
                Update payment
              </button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function Panel({ kicker, title, count, className = "", children }) {
  return (
    <section className={`panel ${className}`.trim()}>
      <div className="panel-header">
        <div>
          <p className="panel-kicker">{kicker}</p>
          <h2>{title}</h2>
        </div>
        {typeof count === "number" ? <span className="count-badge">{count}</span> : null}
      </div>
      {children}
    </section>
  );
}

function MetricCard({ label, value, tone }) {
  return (
    <article className={`metric-card tone-${tone}`}>
      <p>{label}</p>
      <h3>{value}</h3>
    </article>
  );
}

function EmptyState({ title, body }) {
  return (
    <div className="empty-state">
      <strong>{title}</strong>
      <p>{body}</p>
    </div>
  );
}

function LoadingScreen({ title }) {
  return (
    <div className="shell loading-shell">
      <div className="backdrop" />
      <div className="loading-panel">
        <p className="eyebrow">Prithvi Dashboard</p>
        <h1>{title}</h1>
        <p>Connecting to the deployed API and preparing a protected workspace.</p>
      </div>
    </div>
  );
}

function InfoPair({ label, value }) {
  return (
    <div className="info-pair">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function MiniList({ title, items, emptyText }) {
  return (
    <div className="mini-list">
      <h4>{title}</h4>
      {items.length ? (
        <ul>
          {items.map((item) => <li key={item}>{item}</li>)}
        </ul>
      ) : (
        <p>{emptyText}</p>
      )}
    </div>
  );
}

export default App;
