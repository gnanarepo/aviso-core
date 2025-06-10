import math
import random
import string
from collections import defaultdict, deque
from itertools import zip_longest

from utils.misc_utils import iter_chunks

REGION_LEVELS = {'Global': ['APAC', 'AMER'],
                 'APAC': ['Asia', 'Oceania'],
                 'AMER': ['North', 'South'],
                 'Asia': ['China', 'Japan'],
                 'Oceania': ['Australia', 'New Zealand'],
                 'North': ['USA', 'Canada'],
                 'South': ['Brazil', 'Argentina'],
                 'China': ['North China', 'East China'],
                 'Japan': ['Kanto', 'Kansai'],
                 'Australia': ['Victoria', 'New South Wales'],
                 'New Zealand': ['North Island', 'South Island'],
                 'USA': ['East', 'West', ],
                 'East': ['Northeast', 'Southeast'],
                 'West': ['Northwest', 'Southwest'],
                 'Northeast': ['New York', 'Connecticut']
                 }

TYPE_LEVELS = {'Types': ['New', 'Renewal'],
               'New': ['New Prod X', 'New Prod Y'],
               'Renewal': ['Renewal Prod X', 'Renewal Prod Y'],
               'New Prod X': ['New X Region 1', 'New X Region 2'],
               'New Prod Y': ['New Y Region 1', 'New Y Region 2'],
               'Renewal Prod X': ['Renewal X Region 1', 'Renewal X Region 2'],
               'Renewal Prod Y': ['Renewal Y Region 1', 'Renewal Y Region 1']
               }

REPS = ['Abby Dahlkemper',
        'Adrianna Franch',
        'Alex Morgan',
        'Ali Krieger',
        'Allie Long',
        'Alyssa Naeher',
        'Ashlyn Harris',
        'Becky Sauerbrunn',
        'Carli Lloyd',
        'Christen Press',
        'Crystal Dunn',
        'Emily Sonnett',
        'Jessica McDonald',
        'Julie Ertz',
        "Kelley O'Hara",
        'Lindsey Horan',
        'Mallory Pugh',
        'Megan Rapinoe',
        'Morgan Brian',
        'Rose Lavelle',
        'Sam Mewis',
        'Tierna Davidson',
        'Tobin Heath',
        'Adil Rami',
        'Alphonse Areola',
        'Antoine Griezmann',
        'Benjamin Mendy',
        'Benjamin Pavard',
        'Blaise Matuidi',
        'Corentin Tolisso',
        'Djibril Sidib\xc3\xa9',
        'Florian Thauvin',
        'Hugo Lloris',
        'Kylian Mbapp\xc3\xa9',
        'Lucas Hernandez',
        "N'Golo Kant\xc3\xa9",
        'Nabil Fekir',
        'Olivier Giroud',
        'Ousmane Demb\xc3\xa9l\xc3\xa9',
        'Paul Pogba',
        'Presnel Kimpembe',
        'Rapha\xc3\xabl Varane',
        'Samuel Umtiti',
        'Steve Mandanda',
        'Steven Nzonzi',
        'Thomas Lemar',
        'Abby Wambach',
        'Amy Rodriguez',
        'Christie Rampone',
        "Heather O'Reilly",
        'Hope Solo',
        'Lauren Holiday',
        'Lori Chalupny',
        'Meghan Klingenberg',
        'Shannon Boxx',
        'Sydney Leroux',
        'Whitney Engen',
        'Andr\xc3\xa9 Sch\xc3\xbcrrle',
        'Bastian Schweinsteiger',
        'Benedikt H\xc3\xb6wedes',
        'Christoph Kramer',
        'Erik Durm',
        'Julian Draxler',
        'J\xc3\xa9r\xc3\xb4me Boateng',
        'Kevin Gro\xc3\x9fkreutz',
        'Lukas Podolski',
        'Manuel Neuer',
        'Mario G\xc3\xb6tze',
        'Mats Hummels',
        'Matthias Ginter',
        'Mesut \xc3\x96zil',
        'Miroslav Klose',
        'Per Mertesacker',
        'Philipp Lahm',
        'Roman Weidenfeller',
        'Ron-Robert Zieler',
        'Sami Khedira',
        'Shkodran Mustafi',
        'Thomas M\xc3\xbcller',
        'Toni Kroos',
        'Asuna Tanaka',
        'Aya Miyama',
        'Aya Sameshima',
        'Ayumi Kaihori',
        'Azusa Iwashimizu',
        'Homare Sawa',
        'Karina Maruyama',
        'Kozue Ando',
        'Kyoko Yano',
        'Mana Iwabuchi',
        'Megumi Kamionobe',
        'Megumi Takase',
        'Miho Fukumoto',
        'Mizuho Sakaguchi',
        'Nahomi Kawasumi',
        'Nozomi Yamago',
        'Rumi Utsugi',
        'Saki Kumagai',
        'Shinobu Ohno',
        'Yukari Kinga',
        'Y\xc5\xabki Nagasato',
        'Andr\xc3\xa9s Iniesta',
        'Carles Puyol',
        'Carlos Marchena',
        'Cesc F\xc3\xa0bregas',
        'David Silva',
        'David Villa',
        'Fernando Llorente',
        'Fernando Torres',
        'Gerard Piqu\xc3\xa9',
        'Iker Casillas',
        'Javi Mart\xc3\xadnez',
        'Jes\xc3\xbas Navas',
        'Joan Capdevila',
        'Juan Mata',
        'Pedro',
        'Pepe Reina',
        'Ra\xc3\xbal Albiol',
        'Sergio Busquets',
        'Sergio Ramos',
        'V\xc3\xadctor Vald\xc3\xa9s',
        'Xabi Alonso',
        'Xavi',
        '\xc3\x81lvaro Arbeloa', 'Anja Mittag',
        'Annike Krahn',
        'Ariane Hingst',
        'Babett Peter',
        'Birgit Prinz',
        'Fatmire Bajramaj',
        'Kerstin Garefrekes',
        'Kerstin Stegemann',
        'Linda Bresonik',
        'Martina M\xc3\xbcller',
        'Melanie Behringer',
        'Nadine Angerer',
        'Petra Wimbersky',
        'Renate Lingor',
        'Sandra Minnert',
        'Sandra Smisek',
        'Saskia Bartusiak',
        'Silke Rottenberg',
        'Simone Laudehr',
        'Sonja Fuss',
        'Ursula Holl',
        'Alberto Gilardino',
        'Alessandro Del Piero',
        'Alessandro Nesta',
        'Andrea Barzagli',
        'Andrea Pirlo',
        'Angelo Peruzzi',
        'Cristian Zaccardo',
        'Daniele De Rossi',
        'Fabio Cannavaro',
        'Fabio Grosso',
        'Filippo Inzaghi',
        'Francesco Totti',
        'Gennaro Gattuso',
        'Gianluca Zambrotta',
        'Gianluigi Buffon',
        'Luca Toni',
        'Marco Amelia',
        'Marco Materazzi',
        'Massimo Oddo',
        'Mauro Camoranesi',
        'Simone Barone',
        'Simone Perrotta',
        'Vincenzo Iaquinta',
        'Bettina Wiegmann',
        'Conny Pohlers',
        'Maren Meinert',
        'Nia K\xc3\xbcnzer',
        'Pia Wunderlich',
        'Stefanie Gottschlich',
        'Steffi Jones',
        'Viola Odebrecht',
        'Cafu',
        'Den\xc3\xadlson',
        'Dida',
        'Edm\xc3\xadlson',
        'Ed\xc3\xadlson',
        'Gilberto Silva',
        'Juliano Belletti',
        'Juninho Paulista',
        'J\xc3\xbanior',
        'Kak\xc3\xa1',
        'Kl\xc3\xa9berson',
        'Luiz\xc3\xa3o',
        'L\xc3\xbacio',
        'Marcos',
        'Ricardinho',
        'Rivaldo',
        'Roberto Carlos',
        'Rog\xc3\xa9rio Ceni',
        'Ronaldinho',
        'Ronaldo',
        'Roque J\xc3\xbanior',
        'Vampeta',
        '\xc3\x82nderson Polga',
        'Brandi Chastain',
        'Briana Scurry',
        'Carla Overbeck',
        'Cindy Parlow',
        'Danielle Fotopoulos',
        'Joy Fawcett',
        'Julie Foudy',
        'Kate Sobrero',
        'Kristine Lilly',
        'Lorrie Fair',
        'Mia Hamm',
        'Michelle Akers',
        'Sara Whalen',
        'Saskia Webber',
        'Shannon MacMillan',
        'Tiffany Roberts',
        'Tiffeny Milbrett',
        'Tisha Venturini',
        'Tracy Ducar'
        ]


def dfs(tree, roots):
    to_visit = deque((root, 0) for root in roots)
    children_of = defaultdict(list)
    for node, parent in tree.items():
        children_of[parent].append(node)
    while to_visit:
        next_node, level = to_visit.pop()
        yield next_node, level
        to_visit.extend([(node, level + 1) for node in children_of.get(next_node, [])])


def draw_tree(node_to_parent, labels, input_roots=None):
    roots = [n for n, p in node_to_parent.items() if not p]
    roots = input_roots or roots

    ret = ''
    for node, level in dfs(node_to_parent, roots):
        ret += "  " * level + labels.get(node, node) + "\n"

    return ret


def make_new_hierarchy(rep_count=None, seed=None, alt_hier=False):
    random.seed(seed)
    rep_count = rep_count or 23
    node_to_parent, labels = make_node_to_parent_and_labels(REGION_LEVELS, 'Global', rep_count)

    if alt_hier:
        alt_node_to_parent, alt_labels = make_node_to_parent_and_labels(TYPE_LEVELS, 'Types', rep_count / 4)
        node_to_parent.update(alt_node_to_parent)
        labels.update(alt_labels)

    return node_to_parent, labels


def make_valid_tree(tree, seed=100):
    valid_tree = False
    while not valid_tree:
        new_tree, roots = shuffle_tree(tree, seed)
        valid_tree = validate_tree(new_tree, roots)
        seed += 1
    return new_tree


def make_pruned_tree(tree, seed=100):
    random.seed(seed)
    pruned_tree = {k: v for k, v in tree.iteritems()}
    leaves = set(tree.keys()) - set(tree.values())
    for dead_leaf in random.sample(leaves, 2):
        pruned_tree.pop(dead_leaf)
    return pruned_tree


def graft_pruned_tree(tree, partial_tree):
    new_tree = {k: v for k, v in tree.iteritems()}
    new_tree.update(partial_tree)
    return new_tree


def shuffle_tree(tree, seed):
    random.seed(seed)
    nodes, parents, roots = [], [], []
    for node, parent in tree.items():
        if not parent:
            roots.append(node)
        nodes.append(node)
        parents.append(parent)

    tree_size = len(nodes)
    shuffles = int(round(math.log(tree_size, 2)))
    shuffle_size = int(tree_size / 10)

    for _ in range(shuffles):
        index = random.randint(0, int(tree_size - (shuffle_size + 1)))
        nodes[index:index + shuffle_size] = random.sample(nodes[index:index + shuffle_size], shuffle_size)
        parents[index:index + shuffle_size] = random.sample(parents[index:index + shuffle_size], shuffle_size)

    return {node: parent for node, parent in zip_longest(nodes + roots, parents, fillvalue=None)}, set(roots)


def validate_tree(tree, roots):
    # checking for cycles
    # and that all parents are in tree
    roots = set(roots)
    children_of, new_roots = defaultdict(list), set()
    for node, parent in tree.items():
        children_of[parent].append(node)
        if not parent:
            new_roots.add(node)
        elif parent not in tree:
            return False

    if roots != new_roots:
        return False

    nodes, states = set(children_of.keys()), {}

    def dfs(node):
        states[node] = 'temp'
        for child in children_of.get(node, []):
            state = states.get(child)
            if state == 'perm':
                continue
            if state == 'temp':
                raise Exception('cycle')

            nodes.discard(child)
            dfs(child)

        states[node] = 'perm'

    while nodes:
        try:
            dfs(nodes.pop())
        except:
            return False

    return True


def make_node_to_parent_and_labels(tree, root, rep_count):
    max_depth = math.log(rep_count, 2)  # some real sloppy math to get 2 reps/flm
    owner_ids = [''.join(['0050{}00'.format(x), ''.join(random.choice(string.ascii_uppercase + string.digits)
                                                        for _ in range(10))])[:15]
                 for x in range(rep_count)]

    stack, node_to_parent = deque([(root, 1)]), {root: None}
    while stack:
        node, depth = stack.pop()
        if depth > max_depth - 1:
            break
        kids, kid_depth = tree.get(node, []), depth + 1
        node_to_parent.update({kid: node for kid in kids})
        stack.extendleft([(kid, kid_depth) for kid in kids])

    nodes, parents = node_to_parent.keys(), node_to_parent.values()
    first_line_managers = [node for node in nodes if node not in parents]

    chunk_size = int(round(1. * rep_count / len(first_line_managers)))
    for nodes, parent in zip_longest(list(iter_chunks(owner_ids, chunk_size)),
                                     first_line_managers,
                                     fillvalue=first_line_managers[-1]):
        node_to_parent.update({node: parent for node in nodes})

    labels = REPS[:rep_count]
    random.shuffle(labels)

    return node_to_parent, {owner_id: label for owner_id, label in zip(owner_ids, labels)}
