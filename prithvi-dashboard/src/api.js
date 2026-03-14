const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || "https://prithvi-s41b.onrender.com";

export const TOKEN_KEY = "prithvi_access_token";

export function getStoredToken() {
  return window.localStorage.getItem(TOKEN_KEY) || "";
}

export function setStoredToken(token) {
  if (token) {
    window.localStorage.setItem(TOKEN_KEY, token);
    return;
  }
  window.localStorage.removeItem(TOKEN_KEY);
}

export async function apiRequest(path, options = {}) {
  const { token, body, headers, ...rest } = options;
  const response = await fetch(`${API_BASE_URL}${path}`, {
    ...rest,
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...headers,
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  const text = await response.text();
  const data = text ? JSON.parse(text) : null;

  if (!response.ok) {
    const message = data?.detail || data?.error || `${path} returned ${response.status}`;
    const error = new Error(message);
    error.status = response.status;
    error.data = data;
    throw error;
  }

  return data;
}

export { API_BASE_URL };
