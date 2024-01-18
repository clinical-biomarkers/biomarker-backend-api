''' Defines the data models for the biomarker API. Currently written for schema v0.3.1. 
'''

from flask_restx import Namespace, fields

api = Namespace('dataset', description = 'Dataset operations API')

### define the biomarker component models

simple_synonym_model = api.model('SimpleSynonym', {
    'synonym': fields.String(
        required = False,
        description = 'The synonym.'
    )
})

assessed_biomarker_entity_model = api.model('AssessedBiomarkerEntity', {
    'recommended_name': fields.String(
        required = True,
        description = 'The recommended name of the biomarker entity.'
    ),
    'synonym': fields.List(
        fields.Nested(
            simple_synonym_model,
            default = []
        )
    )
})

specimen_model = api.model('Specimen', {
    'name': fields.String(
        required = False,
        description = 'The specimen name.'
    ),
    'specimen_id': fields.String(
        required = False,
        description = 'The specimen name space and ID.'
    ),
    'name_space': fields.String(
        required = False,
        description = 'The name space of the specimen ID.'
    ),
    'url': fields.String(
        required = False,
        description = 'The URL for the specimen within the name space resource.'
    ),
    'loinc_code': fields.String(
        required = False,
        description = 'The LOINC code for the specimen.'
    )
})

evidence_list_model = api.model('EvidenceList', {
    fields.String(
        required = True,
        description = 'The evidence.'
    )
})

tag_model = api.model('Tag', {
    fields.String(
        required = True,
        description = 'The tag.'
    )
})

evidence_source_model = api.model('EvidenceSource', {
    'evidence_id': fields.String(
        required = True,
        description = 'The evidence ID.'
    ),
    'database': fields.String(
        required = True,
        description = 'The database the evidence is from.'
    ),
    'url': fields.String(
        required = False,
        description = 'The URL for the evidence source.'
    ),
    'evidence_list': fields.List(
        fields.Nested(
            evidence_list_model,
            required = True
        )
    ),
    'tags': fields.List(
        fields.Nested(
            tag_model,
            required = True
        )
    )
})

condition_recommended_name_model = api.model('ConditionRecommendedName', {
    'condition_id': fields.String(
        required = True,
        description = 'The condition resource identifier and ID.'
    ),
    'name': fields.String(
        required = True,
        description = 'The recommended name of the condition.'
    ),
    'description': fields.String(
        required = False,
        description = 'The description of the condition.'
    ),
    'resource': fields.String(
        required = False,
        description = 'The resource for the condition.'
    ),
    'url': fields.String(
        required = False,
        description = 'The URL to the condition in the resource.'
    )
})

condition_synonym_model = api.model('ConditionSynonym', {
    'synonym_id': fields.String(
        required = False,
        description = 'The synonym resource identifier and ID.'
    ),
    'name': fields.String(
        required = False,
        description = 'The synonym name.'
    ),
    'resource': fields.String(
        required = False,
        description = 'The resource for the synonym.'
    ),
    'url': fields.String(
        required = False,
        description = 'The URL to the synonym in the resource.'
    )
})

exposure_agent_recommended_name_model = api.model('ExposureAgentRecommendedName', {
    'exposure_agent_id': fields.String(
        required = True,
        description = 'The exposure agent resource identifier and ID.'
    ),
    'name': fields.String(
        required = True,
        description = 'The recommended name of the exposure agent.'
    ),
    'description': fields.String(
        required = False,
        description = 'The description of the exposure agent.'
    ),
    'resource': fields.String(
        required = False,
        description = 'The resource for the exposure agent.'
    ),
    'url': fields.String(
        required = False,
        description = 'The URL to the exposure agent in the resource.'
    )
})

reference_model = api.model('Reference', {
    'reference_id': fields.String(
        required = False,
        description = 'The reference ID.'
    ),
    'type': fields.String(
        required = False,
        description = 'The reference type.'
    ),
    'url': fields.String(
        required = False,
        description = 'The URL to the reference.'
    )
})

simple_evidence_source_model = api.model('SimpleEvidenceSource', {
    'evidence_id': fields.String(
        required = False,
        description = 'The evidence ID.'
    ),
    'database': fields.String(
        required = False,
        description = 'The database the evidence is from.'
    ),
    'url': fields.String(
        required = False,
        description = 'The URL for the evidence source.'
    )
})

### top level object models

biomarker_component_model = api.model('BiomarkerComponent', {
    'biomarker': fields.String(
        required = True,
        description = 'The entity change.'
    ),
    'assessed_biomarker_entity': fields.Nested(
        assessed_biomarker_entity_model,
        required = True
    ),
    'assessed_entity_type': fields.String(
        required = True,
        description = 'The entity type.'
    ),
    'specimen': fields.List(
        fields.Nested(
            specimen_model
        ),
        required = False
    ),
    'evidence_source': fields.List(
        fields.Nested(
            evidence_source_model
        ),
        default = []
    )
})

biomarker_role_model = api.model('BiomarkerRole', {
    'role': fields.String(
        required = True,
        description = 'The role of the biomarker.'
    ),
})

condition_model = api.model('Condition', {
    'condition_id': fields.String(
        required = True,
        description = 'The condition resource identifier and ID.'
    ),
    'recommended_name': fields.Nested(
        condition_recommended_name_model,
        required = True
    ),
    'synonyms': fields.List(
        fields.Nested(
            condition_synonym_model
        ),
        default = []
    ),
})

exposure_agent_model = api.model('ExposureAgent', {
    'exposure_agent_id': fields.String(
        required = True,
        description = 'The exposure agent resource identifier and ID.'
    ),
    'recommended_name': fields.Nested(
        exposure_agent_recommended_name_model,
        required = True
    )
})

citation_model = api.model('Citation', {
    'citation_title': fields.String(
        required = False,
        description = 'The title of the citation.'
    ),
    'journal': fields.String(
        required = False,
        description = 'The journal the citation is from.'
    ),
    'authors': fields.String(
        required = False,
        description = 'The authors of the citation.'
    ),
    'date': fields.String(
        required = False,
        description = 'The date of the citation.'
    ),
    'reference': fields.List(
        fields.Nested(
            reference_model,
            required = False
        )
    ),
    'evidence_source': fields.List(
        fields.Nested(
            simple_evidence_source_model
        ),
        default = []
    )
})

### define the top level data model

data_model = api.model('DataModel', {
    'biomarker_id': fields.String(
        required = True,
        description = 'The unique ID for the biomarker.'
    ),
    'biomarker_component': fields.List(
        fields.Nested(
            biomarker_component_model
        ),
        required = True
    ),
    'best_biomarker_role': fields.List(
        fields.Nested(
            biomarker_role_model,
            required = True
        )
    ),
    'condition': fields.Nested(
        condition_model,
        required = False
    ),
    'exposure_agent': fields.Nested(
        exposure_agent_model,
        required = False
    ),
    'evidence_source': fields.List(
        fields.Nested(
            evidence_source_model
        ),
        default = []
    ),
    'citation': fields.List(
        fields.Nested(
            citation_model
        ),
        default = []
    )
})