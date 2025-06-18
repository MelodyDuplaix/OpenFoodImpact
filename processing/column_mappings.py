"""
Mappages centralisés des noms de colonnes pour les scripts ETL.
Permet de traduire les noms de colonnes des sources externes vers les noms utilisés dans la base de données.
"""

AGRIBALYSE_MAPPING = {
    "Écotoxicité_pour_écosystèmes_aquatiques_d'eau_douce": "ecotoxicite_eau_douce",
    "Code_AGB": "code_agb",
    "Épuisement_des_ressources_énergétiques": "epuisement_ressources_energetiques",
    "Eutrophisation_marine": "eutrophisation_marine",
    "Sous-groupe_d'aliment": "sous_groupe_aliment",
    "Effets_toxicologiques_sur_la_santé_humaine___substances_cancérogènes": "effets_tox_cancerogenes",
    "Approche_emballage_": "approche_emballage",
    "Code_CIQUAL": "code_ciqual",
    "LCI_Name": "lci_name",
    "Nom_du_Produit_en_Français": "nom_produit_francais",
    "Épuisement_des_ressources_eau": "epuisement_ressources_eau",
    "Eutrophisation_terrestre": "eutrophisation_terrestre",
    "Utilisation_du_sol": "utilisation_sol",
    "code_avion": "code_avion",
    "Effets_toxicologiques_sur_la_santé_humaine___substances_non-cancérogènes": "effets_tox_non_cancerogenes",
    "Changement_climatique": "changement_climatique",
    "Épuisement_des_ressources_minéraux": "epuisement_ressources_mineraux",
    "Particules_fines": "particules_fines",
    "Formation_photochimique_d'ozone": "formation_photochimique_ozone",
    "Livraison": "livraison",
    "Préparation": "preparation",
    "Changement_climatique_-_émissions_biogéniques": "changement_climatique_biogenique",
    "Acidification_terrestre_et_eaux_douces": "acidification_terrestre_eaux_douces",
    "Groupe_d'aliment": "groupe_aliment",
    "Changement_climatique_-_émissions_liées_au_changement_d'affectation_des_sols": "changement_climatique_cas",
    "Score_unique_EF": "score_unique_ef",
    "Appauvrissement_de_la_couche_d'ozone": "appauvrissement_couche_ozone",
    "Rayonnements_ionisants": "rayonnements_ionisants",
    "Eutrophisation_eaux_douces": "eutrophisation_eaux_douces",
    "Changement_climatique_-_émissions_fossiles": "changement_climatique_fossile",
    "_score": "score"
}
