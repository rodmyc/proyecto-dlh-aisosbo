export const DATASETS = {
  campaigns: {
    label: "Campañas",
    description: "Campañas para donantes individuales y corporativos.",
    extension: ".csv",
  },
  contacts: {
    label: "Contactos",
    description: "Información vigente de donantes individuales.",
    extension: ".csv",
  },
  accounts: {
    label: "Cuentas",
    description: "Información vigente de donantes empresariales.",
    extension: ".csv",
  },
  recurring_donations: {
    label: "Compromisos",
    description: "Compromisos recurrentes asumidos por los donantes.",
    extension: ".csv",
  },
  opportunities: {
    label: "Donaciones Salesforce",
    description: "Oportunidades con estados cobrados, perdidos y pendientes.",
    extension: ".csv",
  },
  historical_donations: {
    label: "Histórico de donaciones",
    description: "Base consolidada de pagos efectivos anteriores a 2026.",
    extension: ".parquet",
  },
} as const;

export type DatasetKey = keyof typeof DATASETS;

export function isDatasetKey(value: string): value is DatasetKey {
  return Object.hasOwn(DATASETS, value);
}
