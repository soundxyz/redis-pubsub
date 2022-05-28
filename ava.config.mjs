export default {
  files: ["**/*.test.ts", "**/*.spec.ts"],
  serial: true,
  verbose: true,
  extensions: {
    ts: "module",
  },
  serial: true,
  verbose: true,
  nodeArguments: ["--require=bob-tsm", "--loader=bob-tsm"],
  require: ["dotenv/config"],
};
