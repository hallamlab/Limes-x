import os, sys
from pathlib import Path
import pytest

from limes_x.models import Application, Namespace, Transform, Endpoint, Dependency, Solve
from helpers import ConditionalSkip

def _set(s: str):
    return set(s.split(", "))

@ConditionalSkip(__file__)
class Test_solver:
    # https://docs.pytest.org/en/latest/how-to/xunit_setup.html
    def setup_method(self, method):
        self.ns = Namespace()

    def teardown_method(self, method):
        pass

    # makes the creation of transforms less verbose
    def _tr(self, inputs, outputs):
        t = Transform(self.ns)
        def _add(source, fn):
            deps:list[Dependency] = []
            for entry in source:
                if isinstance(entry, str):
                    rprops, rparents = entry, []
                else:
                    rprops, rparents = entry
                props = rprops.split(", ")
                parents = {deps[i] for i in rparents}
                fn(props, parents)
        _add(inputs, lambda *args: t.AddRequirement(*args))
        _add(outputs, lambda *args: t.AddProduct(*args))
        return t

    # https://docs.pytest.org/en/latest/explanation/fixtures.html#about-fixtures
    @pytest.fixture
    def pool_a(self):
        return [Endpoint(self.ns, {"a"})]

    @pytest.fixture
    def pool_s(self):
        return [Endpoint(self.ns, {"s"})]
    
    @pytest.fixture
    def target_x(self):
        return self._tr(["x"], [])

    @pytest.fixture
    def library_single(self):
        return [
            self._tr(["a"], ["x"]),
        ]

    @pytest.fixture
    def library_chain3(self):
        return [
            self._tr(["a"], ["b"]),
            self._tr(["b"], ["c"]),
            self._tr(["c"], ["x"]),
        ]

    def _check(self, plan: list[Application], given: list[Endpoint], target: Transform):
        have = {e for e in given}
        remaining_targets = target.requires.copy()
        def _check_remove(proto):
            found = None
            for t in remaining_targets:
                if proto.IsA(t):
                    found = t
                    break
            if found is not None:
                remaining_targets.remove(found)

        for e in have:
            _check_remove(e)

        for app in plan:
            print(f"{app}")
            for u in app.used:
                assert u in have, f"haven't made [{u}] yet"
            have |= app.produced.keys()

            for e, p in app.produced.items():
                _check_remove(p)

        for d in remaining_targets:
            print(f"missing [{d}]")
        assert len(remaining_targets) == 0, f"missing [{len(remaining_targets)}]"

    def test_single(self, pool_a, target_x):
        transforms = [
            self._tr(["a"], ["x"]),
        ]
        results = Solve(pool_a, target_x, transforms)
        assert len(results) > 0
        self._check(results[0].dependency_plan, pool_a, target_x)

    def test_chain3(self, pool_a, target_x):
        transforms = [
            self._tr(["a"], ["b"]),
            self._tr(["b"], ["c"]),
            self._tr(["c"], ["x"]),
        ]
        results = Solve(pool_a, target_x, transforms)
        assert len(results) > 0
        self._check(results[0].dependency_plan, pool_a, target_x)

    def test_complex_atomic(self, pool_s, target_x):
        transforms = [
            self._tr({'s'}, {'a', '_a'}),

            self._tr({'a'}, {'b'}),
            self._tr({'a'}, {'y'}),
            self._tr({'b'}, {'c'}),
            self._tr({'c'}, {'a'}),
            self._tr({'c'}, {'x'}),
            self._tr({'j'}, {'x'}),

            self._tr({'_a'}, {'_b'}),
            self._tr({'_a'}, {'_y'}),
            self._tr({'_a'}, {'_c'}),
            self._tr({'_b', '_c'}, {'_x'}),
            self._tr({'_j'}, {'_x'}),

            self._tr({'b', '_b'}, {'+1'}),
            self._tr({'c', '_b', 'a'}, {'+2', 'a'}),
            self._tr({'+1', '+2', '_y', '_c', 'c'}, {'j', '+3', 'x'}),

            self._tr({'+1', '+2', '+3'}, {'+x'})
        ]
        results = Solve(pool_s, target_x, transforms)
        assert len(results) > 0
        self._check(results[0].dependency_plan, pool_s, target_x)

    def test_asymm_diamond(self, pool_s, target_x):
        transforms = [
            self._tr({'s'}, {'ss'}),
            self._tr({'ss'}, {'a'}),
            self._tr({'a'}, {'b'}),
            self._tr({'b'}, {'c'}),
            self._tr({'a'}, {'y'}),

            self._tr({'a'}, {'_c'}),
            self._tr({'_c', 'c'}, {'x'}),
            self._tr({'j'}, {'x'}),
        ]
        results = Solve(pool_s, target_x, transforms)
        assert len(results) > 0
        self._check(results[0].dependency_plan, pool_s, target_x)
        assert len(results[0].dependency_plan) == 6

    def test_should_pick_shorter_path(self, pool_s, target_x):
        transforms = [
            self._tr({'s'}, {'a', 'i'}),

            self._tr({'i'}, {'j'}),
            self._tr({'j'}, {'k'}),
            self._tr({'k'}, {'x'}),

            self._tr({'a'}, {'b'}),
            self._tr({'b'}, {'x'}),
        ]
        results = Solve(pool_s, target_x, transforms)
        assert len(results) > 0
        self._check(results[0].dependency_plan, pool_s, target_x)
        assert len(results[0].dependency_plan) == 3

    def test_simple_generic_type(self):
        transforms = []
        NS = self.ns

        t = Transform(NS)
        t.AddRequirement({"genomic", "contigs"})
        t.AddProduct({"orfs"})
        transforms.append(t)

        t = Transform(NS)
        t.AddRequirement({"protein reference"})
        t.AddRequirement({"orfs"})
        t.AddProduct({"annotations"})
        transforms.append(t)

        have = [
            Endpoint(NS, {"genomic", "contigs"}),
            Endpoint(NS, {"protein reference", "KEGG"}),
            Endpoint(NS, {"protein reference", "COG"}),
            Endpoint(NS, {"protein reference", "metacyc"}),
        ]

        target = Transform(NS)
        kegg = target.AddRequirement({"protein reference", "KEGG"})
        metacyc = target.AddRequirement({"protein reference", "metacyc"})
        target.AddRequirement({"annotations"}, {kegg})
        target.AddRequirement({"annotations"}, {metacyc})

        results = Solve(have, target, transforms)
        assert len(results) > 0
        self._check(results[0].dependency_plan, have, target)

    def test_complex_generic_type(self):
        transforms = []
        NS = self.ns

        t = Transform(NS)
        t.AddRequirement(_set("dna"))
        t.AddProduct(_set("contigs, assembly, genomic"))
        transforms.append(t)

        t = Transform(NS)
        r = t.AddRequirement(_set("dna"))
        t.AddRequirement(_set("contigs, assembly"), {r})
        t.AddProduct(_set("contigs, bin, genomic"))
        transforms.append(t)

        t = Transform(NS)
        t.AddRequirement(_set("reference"))
        t.AddRequirement(_set("genomic"))
        t.AddProduct(_set("annotation"))
        transforms.append(t)

        t = Transform(NS)
        genome = t.AddRequirement(_set("genomic"))
        ref_cog = t.AddRequirement(_set("reference, COG"))
        t.AddRequirement(_set("annotation"), {ref_cog, genome})
        ref_kegg = t.AddRequirement(_set("reference, KEGG"))
        t.AddRequirement(_set("annotation"), {ref_kegg, genome})
        t.AddProduct(_set("table"))
        transforms.append(t)

        t = Transform(NS)
        ref_cog = t.AddRequirement(_set("reference, COG"))
        ref_kegg = t.AddRequirement(_set("reference, KEGG"))
        asm_genome = t.AddRequirement(_set("contigs, assembly"))
        bin_genome = t.AddRequirement(_set("contigs, bin"))
        t.AddRequirement(_set("table"), {asm_genome, ref_cog, ref_kegg})
        t.AddRequirement(_set("table"), {bin_genome, ref_cog, ref_kegg})
        t.AddProduct(_set("summary figure"))
        transforms.append(t)

        have = [
            Endpoint(NS, {"dna", "raw reads"}),
            Endpoint(NS, {"reference", "COG"}),
            Endpoint(NS, {"reference", "KEGG"}),
        ]

        target = Transform(NS)
        target.AddRequirement({"summary figure"})

        results = Solve(have, target, transforms)
        assert len(results) > 0
        self._check(results[0].dependency_plan, have, target)

    def test_complex_generic_type2(self): 
        transforms = []
        NS = self.ns

        t = Transform(NS)
        t.AddRequirement(_set("dna"))
        t.AddProduct(_set("contigs, asm, annable"))
        transforms.append(t)

        t = Transform(NS)
        r = t.AddRequirement(_set("dna"))
        t.AddRequirement(_set("contigs, asm"), {r})
        t.AddProduct(_set("contigs, bin, annable"))
        transforms.append(t)

        t = Transform(NS)
        t.AddRequirement(_set("db"))
        t.AddRequirement(_set("annable"))
        t.AddProduct(_set("ann"))
        transforms.append(t)

        t = Transform(NS)
        ann = t.AddRequirement(_set("annable"))
        r = t.AddRequirement(_set("db, cog"))
        t.AddRequirement(_set("ann"), {r, ann})
        r = t.AddRequirement(_set("db, kegg"))
        t.AddRequirement(_set("ann"), {r, ann})
        t.AddProduct(_set("table"))
        transforms.append(t)

        t = Transform(NS)
        db1 = t.AddRequirement(_set("db, cog"))
        db2 = t.AddRequirement(_set("db, kegg"))
        asm = t.AddRequirement(_set("contigs, asm"))
        bin = t.AddRequirement(_set("contigs, bin"))
        t.AddRequirement(_set("table"), {asm, db1, db2})
        t.AddRequirement(_set("table"), {bin, db1, db2})
        t.AddProduct(_set("figure"))
        transforms.append(t)

        t = Transform(NS)
        t.AddRequirement(_set("precog"))
        t.AddProduct(_set("db, cog"))
        transforms.append(t)

        haves = [Endpoint(NS, _set(r)) for r in [
            "precog",
            "db, kegg",
            "dna",
        ]]

        target = Transform(NS)
        target.AddRequirement(_set("figure"))

        results = Solve(haves, target, transforms)
        assert len(results) > 0
        self._check(results[0].dependency_plan, haves, target)

    def test_forward_and_back(self):
        transforms = []
        NS = self.ns

        t = Transform(NS) # assembly <-> bins
        t.AddRequirement({"assembly"})
        t.AddProduct({"bins"})
        transforms.append(t)
        t = Transform(NS)
        t.AddRequirement({"bins"})
        t.AddProduct({"assembly"})
        transforms.append(t)

        t = Transform(NS) # bins <-> tax
        t.AddRequirement({"bins"})
        t.AddProduct({"tax"})
        transforms.append(t)
        t = Transform(NS)
        t.AddRequirement({"tax"})
        t.AddProduct({"bins"})
        transforms.append(t)

        t = Transform(NS) # bins <-> contigs
        t.AddRequirement({"bins"})
        t.AddProduct({"contigs"})
        transforms.append(t)
        t = Transform(NS)
        t.AddRequirement({"contigs"})
        t.AddProduct({"bins"})
        transforms.append(t)

        t = Transform(NS) # contigs <-> ORFs
        t.AddRequirement({"contigs"})
        t.AddProduct({"ORFs"})
        transforms.append(t)
        t = Transform(NS)
        t.AddRequirement({"ORFs"})
        t.AddProduct({"contigs"})
        transforms.append(t)

        t = Transform(NS) # ORFs <-> annotation
        t.AddRequirement({"ORFs"})
        t.AddProduct({"annotation"})
        transforms.append(t)
        t = Transform(NS) 
        t.AddRequirement({"annotation"})
        t.AddProduct({"ORFs"})
        transforms.append(t)


        have = [
            Endpoint(NS, {"annotation"}),
        ]

        target = Transform(NS)
        tax = target.AddRequirement({"tax"})
        target.AddRequirement({"annotation"}, {tax})

        results = Solve(have, target, transforms)
        assert len(results) > 0
        self._check(results[0].dependency_plan, have, target)

    def test_many_generic(self):
        NS = self.ns
        transforms = []

        t = Transform(NS)
        t.AddRequirement(_set("reads"))
        t.AddProduct(_set("annable, taxable"))
        transforms.append(t)

        t = Transform(NS)
        t.AddRequirement(_set("annable"))
        t.AddProduct(_set("ann"))
        transforms.append(t)

        t = Transform(NS)
        t.AddRequirement(_set("taxable"))
        t.AddProduct(_set("tax"))
        transforms.append(t)

        t = Transform(NS)
        d_parent = t.AddRequirement(_set("annable, taxable"))
        d_ann = t.AddRequirement(_set("ann"), {d_parent})
        d_tax = t.AddRequirement(_set("tax"), {d_parent})
        t.AddProduct(_set("sum"))
        transforms.append(t)

        M, N = 128, 8 # M generalized options, find and use N specific options from M
        haves = [Endpoint(NS, _set(f"{i+1}, reads")) for i in range(M)]

        target = Transform(NS)
        # for e in haves[-N:]:
        for e in haves[:N]:
            de = target.AddRequirement(e.properties)
            target.AddRequirement(_set("sum"), {de})

        results = Solve(haves, target, transforms)
        assert len(results) > 0
        self._check(results[0].dependency_plan, haves, target)

