targetScope = 'resourceGroup'

@description('Globally unique storage account name (3-24 lowercase alphanumeric).')
param storageAccountName string

@description('Azure region for the storage account.')
param location string = resourceGroup().location

@description('Storage account SKU.')
@allowed([
  'Standard_LRS'
  'Standard_GRS'
  'Standard_RAGRS'
  'Standard_ZRS'
  'Standard_GZRS'
  'Standard_RAGZRS'
])
param skuName string = 'Standard_LRS'

@description('Whether static website hosting is enabled on the $web container.')
param enableStaticWebsite bool = true

@description('Static website index document.')
param indexDocument string = 'index.html'

@description('Static website 404 document path.')
param errorDocument404Path string = '404.html'

@description('Optional resource tags.')
param tags object = {}

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  sku: {
    name: skuName
  }
  kind: 'StorageV2'
  tags: tags
  properties: {
    accessTier: 'Hot'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: true
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    publicNetworkAccess: 'Enabled'
  }
}

resource storageAccountContributorRole 'Microsoft.Authorization/roleDefinitions@2018-01-01-preview' existing = {
  scope: subscription()
  // Storage Account Contributor
  name: '17d1049b-9a84-46fb-8f53-869881c3d3ab'
}

resource staticWebsiteIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = if (enableStaticWebsite) {
  name: '${storageAccountName}-static-web-mi'
  location: location
}

resource staticWebsiteRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (enableStaticWebsite) {
  scope: storageAccount
  name: guid(storageAccount.id, staticWebsiteIdentity!.id, storageAccountContributorRole.id)
  properties: {
    roleDefinitionId: storageAccountContributorRole.id
    principalId: staticWebsiteIdentity!.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource staticWebsiteEnabler 'Microsoft.Resources/deploymentScripts@2023-08-01' = if (enableStaticWebsite) {
  name: '${storageAccountName}-enable-static-web'
  location: location
  kind: 'AzureCLI'
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${staticWebsiteIdentity!.id}': {}
    }
  }
  dependsOn: [
    staticWebsiteRoleAssignment
  ]
  properties: {
    azCliVersion: '2.61.0'
    retentionInterval: 'P1D'
    timeout: 'PT30M'
    forceUpdateTag: '${indexDocument}-${errorDocument404Path}'
    environmentVariables: [
      {
        name: 'STORAGE_ACCOUNT_NAME'
        value: storageAccount.name
      }
      {
        name: 'INDEX_DOCUMENT'
        value: indexDocument
      }
      {
        name: 'ERROR_DOCUMENT_404_PATH'
        value: errorDocument404Path
      }
    ]
    scriptContent: '''
      set -euo pipefail

      az storage blob service-properties update \
        --account-name "$STORAGE_ACCOUNT_NAME" \
        --auth-mode login \
        --static-website \
        --index-document "$INDEX_DOCUMENT" \
        --404-document "$ERROR_DOCUMENT_404_PATH"
    '''
  }
}

output storageAccountId string = storageAccount.id
output storageAccountResourceName string = storageAccount.name
output staticWebsiteEnabled bool = enableStaticWebsite
output staticWebsiteEndpoint string = storageAccount.properties.primaryEndpoints.web
