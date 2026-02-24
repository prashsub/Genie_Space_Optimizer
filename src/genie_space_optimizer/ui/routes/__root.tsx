import { ThemeProvider } from "@/components/apx/theme-provider";
import NavbarUser from "@/components/navbar-user";
import { QueryClient } from "@tanstack/react-query";
import {
  createRootRouteWithContext,
  Link,
  Outlet,
  useRouterState,
} from "@tanstack/react-router";
import { Toaster } from "sonner";
import { Zap, Settings } from "lucide-react";

export const Route = createRootRouteWithContext<{
  queryClient: QueryClient;
}>()({
  component: RootLayout,
});

function RootLayout() {
  const pathname = useRouterState({ select: (s) => s.location.pathname });

  return (
    <ThemeProvider defaultTheme="light" storageKey="apx-ui-theme">
      <div className="flex min-h-screen flex-col bg-db-gray-bg">
        <header className="sticky top-0 z-50 bg-db-dark text-white shadow-md">
          <div className="mx-auto flex h-14 max-w-7xl items-center justify-between px-4">
            <Link to="/" className="flex items-center gap-2.5 hover:opacity-90">
              <div className="flex h-8 w-8 items-center justify-center rounded-md bg-db-red">
                <Zap className="h-5 w-5 text-white" />
              </div>
              <span className="text-lg font-semibold tracking-tight">
                Genie Space Optimizer
              </span>
            </Link>

            <div className="flex items-center gap-4">
              <nav className="flex items-center gap-1">
                <Link
                  to="/"
                  className={`rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
                    pathname === "/"
                      ? "bg-white/15 text-white"
                      : "text-white/70 hover:bg-white/10 hover:text-white"
                  }`}
                >
                  Dashboard
                </Link>
                <Link
                  to="/settings"
                  className={`flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
                    pathname === "/settings"
                      ? "bg-white/15 text-white"
                      : "text-white/70 hover:bg-white/10 hover:text-white"
                  }`}
                >
                  <Settings className="h-3.5 w-3.5" />
                  Settings
                </Link>
              </nav>

              <div className="h-5 w-px bg-white/20" />
              <NavbarUser />
            </div>
          </div>
        </header>

        <main className="mx-auto w-full max-w-7xl flex-1 px-4 py-6">
          <Outlet />
        </main>

        <footer className="border-t border-db-gray-border bg-white py-4 text-center text-xs text-muted-foreground">
          &copy; {new Date().getFullYear()} Databricks &mdash; Genie Space
          Optimizer
        </footer>
      </div>
      <Toaster richColors />
    </ThemeProvider>
  );
}
