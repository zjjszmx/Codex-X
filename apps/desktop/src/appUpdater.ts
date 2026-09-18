export type AppUpdaterPhase =
  | "idle"
  | "checking"
  | "available"
  | "downloading"
  | "installing"
  | "ready"
  | "error";

export type AppUpdaterState = Readonly<{
  phase: AppUpdaterPhase;
  currentVersion: string | null;
  latestVersion: string | null;
  notes: string | null;
  publishedAt: string | null;
  downloadedBytes: number;
  totalBytes: number | null;
  error: string | null;
  failure: "check" | "download" | "install" | "restart" | null;
}>;

export const INITIAL_APP_UPDATER_STATE: AppUpdaterState = {
  phase: "idle",
  currentVersion: null,
  latestVersion: null,
  notes: null,
  publishedAt: null,
  downloadedBytes: 0,
  totalBytes: null,
  error: null,
  failure: null,
};
