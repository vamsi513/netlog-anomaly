// eslint-config-next 16 ships native flat configs, so they are spread in
// directly rather than going through the eslintrc compatibility shim.
import coreWebVitals from "eslint-config-next/core-web-vitals";
import typescriptConfig from "eslint-config-next/typescript";

const config = [
  { ignores: [".next/**", "node_modules/**", "next-env.d.ts"] },
  ...coreWebVitals,
  ...typescriptConfig,
];

export default config;
