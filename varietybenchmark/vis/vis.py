import matplotlib.pyplot as plt
import seaborn as sns 


def plot_ba_percentage_coincidences(df, logging):
    
    products_coincidence = {}
    for cat, this_ba_cat in df.groupby('BA_category'): #.get_group('Bebidas y Jugos')
        n_ba = len(this_ba_cat)
        n_neta = len(this_ba_cat[this_ba_cat['is_in']=='ambos'])
        perc = n_neta*100/n_ba
        products_coincidence[cat] = n_neta*100/n_ba
        
    products_coincidence = {k: v for k, v in sorted(products_coincidence.items(), key=lambda item: item[1], reverse=True)}    


    keys = list(products_coincidence.keys())
    # get values in the same order as keys, and parse percentage values
    vals = [products_coincidence[k] for k in keys]


    plt.figure(figsize=(8,5))
    plt.grid(alpha=0.3)
    sns.barplot(x=keys, y=vals);
    plt.title('Coincidencias con productos de Neta por categorías de Bodega Aurrerá')
    plt.ylabel('% de coincidencias')
    plt.xticks(rotation=90);
    plt.tight_layout()

    plt.savefig('BA_coincidencias.jpg')
    logging.info('Figure saved in local directory.')
    
    plt.close()
    return