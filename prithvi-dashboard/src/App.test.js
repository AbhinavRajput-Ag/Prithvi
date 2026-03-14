import { render, screen } from "@testing-library/react";
import App from "./App";

beforeEach(() => {
  window.localStorage.clear();
  global.fetch = jest.fn();
});

afterEach(() => {
  jest.resetAllMocks();
});

test("renders the login form by default", () => {
  render(<App />);
  expect(screen.getByText(/Sign in to your farm operating system/i)).toBeInTheDocument();
  expect(screen.getByRole("button", { name: /sign in/i })).toBeInTheDocument();
});
