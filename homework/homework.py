"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
import os 
import pandas as pd
import zipfile


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
 # pylint: disable=import-outside-toplevel
# flake8: noqa: E501


    input_dir = "./files/input"
    output_dir = "./files/output"
    os.makedirs(output_dir, exist_ok=True)

    # Acumuladores para cada tabla
    clients_all = []
    campaigns_all = []
    economics_all = []

    # Procesar todos los .zip en la carpeta input
    for file in os.listdir(input_dir):
        if file.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(input_dir, file), 'r') as zipf:
                for name in zipf.namelist():
                    if name.endswith(".csv"):
                        with zipf.open(name) as f:
                            df = pd.read_csv(f)

                            # === CLIENT ===
                            df_client = df[[
                                "client_id", "age", "job", "marital",
                                "education", "credit_default", "mortgage"
                            ]].copy()

                            df_client["job"] = df_client["job"].str.replace(r"[.]", "", regex=True)
                            df_client["job"] = df_client["job"].str.replace(r"[-]", "_", regex=True)
                            df_client["education"] = df_client["education"].str.replace(".", "_", regex=False)
                            df_client["education"] = df_client["education"].replace("unknown", pd.NA)
                            df_client["credit_default"] = (df_client["credit_default"] == "yes").astype(int)
                            df_client["mortgage"] = (df_client["mortgage"] == "yes").astype(int)

                            clients_all.append(df_client)

                            # === CAMPAIGN ===
                            df_campaign = df[[
                                "client_id", "number_contacts", "contact_duration",
                                "previous_campaign_contacts", "previous_outcome",
                                "campaign_outcome", "day", "month"
                            ]].copy()

                            df_campaign["previous_outcome"] = (df_campaign["previous_outcome"] == "success").astype(int)
                            df_campaign["campaign_outcome"] = (df_campaign["campaign_outcome"] == "yes").astype(int)

                            # Crear 'last_contact_date' en formato 'YYYY-MM-DD'
                            df_campaign["last_contact_date"] = pd.to_datetime(
                                df_campaign["day"].astype(str) + "-" + df_campaign["month"] + "-2022",
                                format="%d-%b-%Y"
                            ).dt.strftime("%Y-%m-%d")

                            df_campaign = df_campaign.drop(columns=["day", "month"])
                            campaigns_all.append(df_campaign)

                            # === ECONOMICS ===
                            df_econ = df[["client_id", "cons_price_idx", "euribor_three_months"]].copy()
                            economics_all.append(df_econ)

    # Unir datos de múltiples archivos si es necesario
    df_client = pd.concat(clients_all, ignore_index=True)
    df_campaign = pd.concat(campaigns_all, ignore_index=True)
    df_economics = pd.concat(economics_all, ignore_index=True)

    # Guardar resultados
    df_client.to_csv(os.path.join(output_dir, "client.csv"), index=False)
    df_campaign.to_csv(os.path.join(output_dir, "campaign.csv"), index=False)
    df_economics.to_csv(os.path.join(output_dir, "economics.csv"), index=False)

    print("✅ Archivos guardados correctamente en ./files/output")


# Ejecutar si se llama directamente
if __name__ == "__main__":
    clean_campaign_data()
