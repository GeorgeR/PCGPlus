// Fill out your copyright notice in the Description page of Project Settings.

#include "Elements/PCGCopyAttributeElement.h"

#include "Data/PCGPointData.h"
#include "Data/PCGSpatialData.h"
#include "Elements/Metadata/PCGMetadataElementCommon.h"
#include "Metadata/Accessors/IPCGAttributeAccessor.h"
#include "Metadata/Accessors/PCGAttributeAccessorHelpers.h"
#include "Metadata/Accessors/PCGAttributeAccessorKeys.h"
#include "Metadata/Accessors/PCGCustomAccessor.h"
#include "PCGContext.h"
#include "PCGModule.h"
#include "PCGPin.h"

#define LOCTEXT_NAMESPACE "PCGCopyAttributeElement"

namespace UE::PCGPlus::Private
{
	static FName NodeName = TEXT("CopyAttribute");
	static FText NodeTitle = LOCTEXT("NodeTitle", "Copy Attribute");
	static FName SourceLabel = TEXT("Source");
	static FName TargetLabel = TEXT("Target");
	static FName MatchByLabel = TEXT("Match By Attribute");
	static FName SourceMatchLabel = TEXT("Source");
	static FName TargetMatchLabel = TEXT("Target");

	// template <typename T, typename... Ts> constexpr bool is_same_v = (std::is_same_v<T, Ts> && ...)
	template <typename DataType>
	struct TPCGDataTypeConstraint
	{
		static_assert(std::is_base_of_v<UPCGData, DataType>, "DataType must be derived from UPCGData.");

		static bool All(std::initializer_list<const UPCGData*> InDataItems)
		{
			for (const UPCGData* DataItem : InDataItems)
			{
				if (!DataItem || !DataItem->IsA(DataType::StaticClass()))
				{
					return false;
				}
			}
			return true;
		}

		static bool Any(std::initializer_list<const UPCGData*> InDataItems)
		{
			for (const UPCGData* DataItem : InDataItems)
			{
				if (!DataItem || DataItem->IsA(DataType::StaticClass()))
				{
					return true;
				}
			}
			return false;
		}

		static bool None(std::initializer_list<const UPCGData*> InDataItems)
		{
			for (const UPCGData* DataItem : InDataItems)
			{
				if (!DataItem || DataItem->IsA(DataType::StaticClass()))
				{
					return false;
				}
			}
			return true;
		}
	};
}

#if WITH_EDITOR
FName UPCGCopyAttributeSettings::GetDefaultNodeName() const
{
	return UE::PCGPlus::Private::NodeName;
}

FText UPCGCopyAttributeSettings::GetDefaultNodeTitle() const
{
	return UE::PCGPlus::Private::NodeTitle;
}

FText UPCGCopyAttributeSettings::GetNodeTooltipText() const
{
	return Super::GetNodeTooltipText();
}
#endif

EPCGDataType UPCGCopyAttributeSettings::GetCurrentPinTypes(const UPCGPin* InPin) const
{
	// All pins narrow to same type, which is Point if any input is Point, otherwise Spatial
	const bool bAnyArePoint = (GetTypeUnionOfIncidentEdges(UE::PCGPlus::Private::TargetLabel) == EPCGDataType::Point)
							|| (GetTypeUnionOfIncidentEdges(UE::PCGPlus::Private::SourceLabel) == EPCGDataType::Point);

	return bAnyArePoint ? EPCGDataType::Point : EPCGDataType::Spatial;
}

FName UPCGCopyAttributeSettings::AdditionalTaskName() const
{
	const FName SourceAttributeName = SourceAttributeProperty.GetName();
	const FName TargetAttributeName = TargetAttributeProperty.GetName();

	if (SourceAttributeName != TargetAttributeName && TargetAttributeName != NAME_None)
	{
		return FName(FString::Printf(TEXT("%s %s to %s"),
			*UE::PCGPlus::Private::NodeName.ToString(),
			*SourceAttributeName.ToString(),
			*TargetAttributeName.ToString()));
	}
	else
	{
		return FName(FString::Printf(TEXT("%s %s"),
			*UE::PCGPlus::Private::NodeName.ToString(),
			*SourceAttributeName.ToString()));
	}
}

TArray<FPCGPinProperties> UPCGCopyAttributeSettings::InputPinProperties() const
{
	TArray<FPCGPinProperties> PinProperties;
	PinProperties.Emplace(UE::PCGPlus::Private::TargetLabel, EPCGDataType::Spatial, /*bInAllowMultipleConnections=*/ false);
	PinProperties.Emplace(UE::PCGPlus::Private::SourceLabel, EPCGDataType::Spatial, /*bInAllowMultipleConnections=*/ false);

	return PinProperties;
}

TArray<FPCGPinProperties> UPCGCopyAttributeSettings::OutputPinProperties() const
{
	TArray<FPCGPinProperties> PinProperties;
	PinProperties.Emplace(PCGPinConstants::DefaultOutputLabel, EPCGDataType::Spatial);

	return PinProperties;
}

FPCGElementPtr UPCGCopyAttributeSettings::CreateElement() const
{
	return MakeShared<FPCGCopyAttributeElement>();
}

bool FPCGCopyAttributeElement::ExecuteInternal(FPCGContext* Context) const
{
	TRACE_CPUPROFILER_EVENT_SCOPE(FPCGCopyAttributeElement::Execute);

	check(Context);

	const UPCGCopyAttributeSettings* Settings = Context->GetInputSettings<UPCGCopyAttributeSettings>();
	check(Settings);

	TArray<FPCGTaggedData> SourceInputs = Context->InputData.GetInputsByPin(UE::PCGPlus::Private::SourceLabel);
	TArray<FPCGTaggedData> TargetInputs = Context->InputData.GetInputsByPin(UE::PCGPlus::Private::TargetLabel);

	if (SourceInputs.Num() != 1 || TargetInputs.Num() != 1)
	{
		PCGE_LOG(Warning, LogOnly, FText::Format(LOCTEXT("WrongNumberOfInputs", "Source input contains {0} data elements, Target inputs contain {1} data elements, but both should contain precisely 1 data element"), SourceInputs.Num(), TargetInputs.Num()));
		return true;
	}
	
	FPCGTaggedData& SourceInput = SourceInputs[0];
	FPCGTaggedData& TargetInput = TargetInputs[0];

	// UE::PCGPlus::Private::TPCGDataTypeConstraint<UPCGSpatialData> SpatialDataConstraint;
	// if (!UE::PCGPlus::Private::TPCGDataTypeConstraint<UPCGSpatialData>::All({ SourceInput.Data, TargetInput.Data }))
	// {
	// 	if (!UE::PCGPlus::Private::TPCGDataTypeConstraint<UPCGPointData>::All({ SourceInput.Data, TargetInput.Data }))
	// 	{
	// 		PCGE_LOG(Error, GraphAndLog, LOCTEXT("UnsupportedTypes", "Only supports Spatial to Spatial data or Point to Point data"));
	// 		return true;
	// 	}
	// }

	const UPCGSpatialData* SourceSpatialData = Cast<const UPCGSpatialData>(SourceInputs[0].Data);
	const UPCGSpatialData* TargetSpatialData = Cast<const UPCGSpatialData>(TargetInputs[0].Data);

	if (!SourceSpatialData
		|| !TargetSpatialData
		|| SourceSpatialData->IsA<UPCGPointData>() != TargetSpatialData->IsA<UPCGPointData>())
	{
		PCGE_LOG(Error, GraphAndLog, LOCTEXT("UnsupportedTypes", "Only supports Spatial to Spatial data or Point to Point data"));
		return true;
	}

	const UPCGPointData* SourcePointData = Cast<const UPCGPointData>(SourceSpatialData);
	const UPCGPointData* TargetPointData = Cast<const UPCGPointData>(TargetSpatialData);

	const bool bIsPointData = SourcePointData && TargetPointData;
	if (bIsPointData && SourcePointData->GetPoints().Num() != TargetPointData->GetPoints().Num())
	{
		PCGE_LOG(Error, GraphAndLog, LOCTEXT("MismatchingPointCounts", "Source and target do not have the same number of points"));
		return true;
	}

	if (!SourceSpatialData->Metadata)
	{
		PCGE_LOG(Error, GraphAndLog, LOCTEXT("SourceMissingMetadata", "Source does not have metadata"));
		return true;
	}

	const FPCGAttributePropertyInputSelector SourceAttributeProperty = Settings->SourceAttributeProperty.CopyAndFixLast(SourceSpatialData);
	const FPCGAttributePropertyOutputSelector TargetAttributeProperty = Settings->TargetAttributeProperty.CopyAndFixSource(&SourceAttributeProperty, SourceSpatialData);

	const FName SourceAttributeName = SourceAttributeProperty.GetName();
	const FName TargetAttributeName = TargetAttributeProperty.GetName();

	if (SourceAttributeProperty.GetSelection() == EPCGAttributePropertySelection::Attribute && !SourceSpatialData->Metadata->HasAttribute(SourceAttributeName))
	{
		PCGE_LOG(Error, GraphAndLog, FText::Format(LOCTEXT("SourceMissingAttribute", "Source does not have the attribute '{0}'"), FText::FromName(SourceAttributeName)));
		return true;
	}

	UPCGSpatialData* OutputData = TargetSpatialData->DuplicateData();
	check(OutputData->Metadata);

	auto AppendOutputData = [Context, OutputData]()
	{
		TArray<FPCGTaggedData>& Outputs = Context->OutputData.TaggedData;
		FPCGTaggedData& Output = Outputs.Emplace_GetRef();
		Output.Data = OutputData;
	};

	// If it is attribute to attribute, just copy the attributes, if they exist and are valid
	// Only do that if it is really attribute to attribute, without any extra accessor. Any extra accessor will behave as a property.
	const bool bInputHasAnyExtra = !SourceAttributeProperty.GetExtraNames().IsEmpty();
	const bool bOutputHasAnyExtra = !TargetAttributeProperty.GetExtraNames().IsEmpty();
	if (!bInputHasAnyExtra && !bOutputHasAnyExtra && SourceAttributeProperty.GetSelection() == EPCGAttributePropertySelection::Attribute && TargetAttributeProperty.GetSelection() == EPCGAttributePropertySelection::Attribute)
	{
		if (SourceSpatialData == TargetSpatialData
			&& SourceAttributeName == TargetAttributeName)
		{
			// Nothing to do if we try to copy an attribute into itself
			AppendOutputData();
			return true;
		}

		const FPCGMetadataAttributeBase* SourceAttribute = SourceSpatialData->Metadata->GetConstAttribute(SourceAttributeName);
		check(SourceAttribute);

		if (bIsPointData)
		{
			UPCGPointData* OutPointData = CastChecked<UPCGPointData>(OutputData);

			const TArray<FPCGPoint>& SourcePoints = SourcePointData->GetPoints();
			TArray<FPCGPoint>& TargetPoints = OutPointData->GetMutablePoints();

			// Making sure the target attribute doesn't exist in the target
			if (OutPointData->Metadata->HasAttribute(TargetAttributeName))
			{
				OutPointData->Metadata->DeleteAttribute(TargetAttributeName);
			}

			FPCGMetadataAttributeBase* TargetAttribute = OutPointData->Metadata->CopyAttribute(SourceAttribute, TargetAttributeName, /*bKeepParent=*/ false, /*bCopyEntries=*/ false, /*bCopyValues=*/ true);
			if (!TargetAttribute)
			{
				PCGE_LOG(Error, GraphAndLog, FText::Format(LOCTEXT("ErrorCreatingTargetAttribute", "Error while creating target attribute '{0}'"), FText::FromName(TargetAttributeName)));
				return true;
			}

			if (Settings->bMatchByAttribute)
			{
				auto IsIndexAttributeSelector = [](const FPCGAttributePropertySelector& InSelector)
				{
					return InSelector.GetSelection() == EPCGAttributePropertySelection::ExtraProperty
						&& InSelector.GetExtraProperty() == EPCGExtraProperties::Index;
				};
				
				// Target Points
				TArray<TPair<int32, int32>> TargetMatchValueIndexPairs;
				{
					const int32 NumPoints = SourcePoints.Num();

					auto TargetPointAccessor = PCGAttributeAccessorHelpers::CreateConstAccessor(TargetPointData, Settings->TargetMatchAttributeProperty);
					auto TargetPointKeys = PCGAttributeAccessorHelpers::CreateConstKeys(TargetPointData, Settings->TargetMatchAttributeProperty);

					TArray<int32> TargetMatchValues;
					TargetMatchValues.SetNum(NumPoints);

					TArrayView<int32> TargetView = TargetMatchValues;
					if (!TargetPointAccessor->GetRange(TargetView, 0, *TargetPointKeys))
					{
						// @todo: ???
					}
					else
					{
						TargetMatchValueIndexPairs.SetNum(NumPoints);
						for (int32 PointIdx = 0; PointIdx < NumPoints; ++PointIdx)
						{
							TargetMatchValueIndexPairs[PointIdx] = { TargetMatchValues[PointIdx], PointIdx };
						}

						Algo::SortBy(TargetMatchValueIndexPairs, [](const TPair<int32, int32>& InMatchValueIndexPair)
						{
							return InMatchValueIndexPair.Key;
						});
					}
				}

				// Source Points
				TArray<TPair<int32, int32>> SourceMatchValueIndexPairs;
				{
					const int32 NumPoints = SourcePoints.Num();

					auto SourcePointAccessor = PCGAttributeAccessorHelpers::CreateConstAccessor(SourcePointData, Settings->SourceMatchAttributeProperty);
					auto SourcePointKeys = PCGAttributeAccessorHelpers::CreateConstKeys(SourcePointData, Settings->SourceMatchAttributeProperty);

					TArray<int32> SourceMatchValues;
					SourceMatchValues.SetNum(NumPoints);

					TArrayView<int32> SourceView = SourceMatchValues;
					if (!SourcePointAccessor->GetRange(SourceView, 0, *SourcePointKeys))
					{
						// @todo: ???
					}
					else
					{
						SourceMatchValueIndexPairs.SetNum(NumPoints);
						for (int32 PointIdx = 0; PointIdx < NumPoints; ++PointIdx)
						{
							SourceMatchValueIndexPairs[PointIdx] = { SourceMatchValues[PointIdx], PointIdx };
						}

						Algo::SortBy(SourceMatchValueIndexPairs, [](const TPair<int32, int32>& InMatchValueIndexPair)
						{
							return InMatchValueIndexPair.Key;
						});
					}
				}

				// Source to Target Map
				TArray<int32> SourceToTargetMatchIndirection;
				// TArray<TPair<int32, int32>> SourceToTargetMatchIndices;
				{
					const int32 NumPoints = SourcePoints.Num();
					SourceToTargetMatchIndirection.Reserve(NumPoints);

					int32 SourcePairIdx = 0, TargetPairIdx = 0, TargetPairLastMatchIdx = 0;					
					while (SourcePairIdx < NumPoints)
					{
						while (TargetPairIdx < NumPoints)
						{
							// Check for value match
							if (TargetMatchValueIndexPairs[TargetPairIdx].Key == SourceMatchValueIndexPairs[SourcePairIdx].Key)
							{
								SourceToTargetMatchIndirection.Emplace(TargetMatchValueIndexPairs[TargetPairIdx].Value);
								// SortedToTargetMatchIndirection.Emplace(SourceMatchValueIndexPairs[SourcePairIdx].Value, TargetMatchValueIndexPairs[TargetPairIdx].Value);
								TargetPairLastMatchIdx = TargetPairIdx;
								++SourcePairIdx;
								++TargetPairIdx;
								break;
							}
							
							++TargetPairIdx;
						}

						// Reset, start at last matching target
						TargetPairIdx = TargetPairLastMatchIdx;

						// No match found, go to next
						++SourcePairIdx;
					}
				}

				// Get TargetAttribute values sorted by matches
				TArray<int32> TargetValues;
				{
					const int32 NumPoints = SourcePoints.Num();
					
					TargetValues.SetNum(NumPoints);

					auto SourcePointAccessor = PCGAttributeAccessorHelpers::CreateConstAccessor(SourcePointData, Settings->SourceMatchAttributeProperty);
					auto SourcePointKeys = PCGAttributeAccessorHelpers::CreateConstKeys(SourcePointData, Settings->SourceMatchAttributeProperty);

					TArrayView<int32> TargetValuesView = TargetValues;
					if (!SourcePointAccessor->GetRange(TargetValuesView, 0, *SourcePointKeys))
					{
					}

					TArrayView<int32> IndirectionView = SourceToTargetMatchIndirection;

					// ArgSort
					{
						for (int32 Idx = 0; Idx < IndirectionView.Num(); Idx++)
						{
							IndirectionView[Idx] = Idx;
						}

						TArrayView<int32>(IndirectionView.GetData(), IndirectionView.Num()).Sort([&TargetValuesView](const int32& Lhs, const int32& Rhs)
						{
							return TargetValuesView[Lhs] < TargetValuesView[Rhs];
						});
					}
				}

				//
				{
					auto OutputPointAccessor = PCGAttributeAccessorHelpers::CreateAccessor(OutputData, Settings->TargetAttributeProperty);
					auto OutputPointKeys = PCGAttributeAccessorHelpers::CreateKeys(OutputData, Settings->TargetAttributeProperty);

					TArrayView<const int32> TargetValuesView = TargetValues;
					OutputPointAccessor->SetRange(TargetValuesView, 0, *OutputPointKeys);
					// OutputPointAccessor->SetRange()				
				}
			}

			// For Point -> Point, the entry keys may not match the source points, so we explicitly write for all the target points
			for (int32 PointIdx = 0; PointIdx < SourcePoints.Num(); ++PointIdx)
			{
				PCGMetadataEntryKey& TargetKey = TargetPoints[PointIdx].MetadataEntry;
				OutPointData->Metadata->InitializeOnSet(TargetKey);
				check(TargetKey != PCGInvalidEntryKey);
				TargetAttribute->SetValueFromValueKey(TargetKey, SourceAttribute->GetValueKey(SourcePoints[PointIdx].MetadataEntry));
			}
		}
		else
		{
			// If we are copying Spatial -> Spatial, we can just copy the attribute across
			if (!OutputData->Metadata->CopyAttribute(SourceAttribute, TargetAttributeName, /*bKeepParent=*/ false, /*bCopyEntries=*/ true, /*bCopyValues=*/ true))
			{
				PCGE_LOG(Error, GraphAndLog, FText::Format(LOCTEXT("FailedCopyToNewAttribute", "Failed to copy to new attribute '{0}'"), FText::FromName(TargetAttributeName)));
			}
		}

		AppendOutputData();

		return true;
	}

	TUniquePtr<const IPCGAttributeAccessor> InputAccessor = PCGAttributeAccessorHelpers::CreateConstAccessor(SourceSpatialData, SourceAttributeProperty);
	TUniquePtr<const IPCGAttributeAccessorKeys> InputKeys = PCGAttributeAccessorHelpers::CreateConstKeys(SourceSpatialData, SourceAttributeProperty);

	if (!InputAccessor.IsValid() || !InputKeys.IsValid())
	{
		PCGE_LOG(Error, GraphAndLog, LOCTEXT("FailedToCreateInputAccessor", "Failed to create input accessor or iterator"));
		return true;
	}

	// If the target is an attribute, only create a new one if the attribute doesn't already exist or we have any extra.
	// If it exist or have any extra, it will try to write to it.
	if (!bOutputHasAnyExtra && TargetAttributeProperty.GetSelection() == EPCGAttributePropertySelection::Attribute && !OutputData->Metadata->HasAttribute(TargetAttributeName))
	{
		auto CreateAttribute = [OutputData, TargetAttributeName](auto Dummy)
		{
			using AttributeType = decltype(Dummy);
			return PCGMetadataElementCommon::ClearOrCreateAttribute(OutputData->Metadata, TargetAttributeName, AttributeType{}) != nullptr;
		};
		
		if (!PCGMetadataAttribute::CallbackWithRightType(InputAccessor->GetUnderlyingType(), CreateAttribute))
		{
			PCGE_LOG(Error, GraphAndLog, FText::Format(LOCTEXT("FailedToCreateNewAttribute", "Failed to create new attribute '{0}'"), FText::FromName(TargetAttributeName)));
			return true;
		}
	}

	TUniquePtr<IPCGAttributeAccessor> OutputAccessor = PCGAttributeAccessorHelpers::CreateAccessor(OutputData, TargetAttributeProperty);
	TUniquePtr<IPCGAttributeAccessorKeys> OutputKeys = PCGAttributeAccessorHelpers::CreateKeys(OutputData, TargetAttributeProperty);

	if (!OutputAccessor.IsValid() || !OutputKeys.IsValid())
	{
		PCGE_LOG(Error, GraphAndLog, LOCTEXT("FailedToCreateOutputAccessor", "Failed to create output accessor or iterator"));
		return true;
	}

	if (OutputAccessor->IsReadOnly())
	{
		PCGE_LOG(Error, GraphAndLog, FText::Format(LOCTEXT("OutputAccessorIsReadOnly", "Attribute/Property '{0}' is read only."), TargetAttributeProperty.GetDisplayText()));
		return true;
	}

	// At this point, we are ready.
	auto Operation = [&InputAccessor, &InputKeys, &OutputAccessor, &OutputKeys, Context](auto _)
	{
		using OutputType = decltype(_);
		OutputType Value{};

		EPCGAttributeAccessorFlags Flags = EPCGAttributeAccessorFlags::AllowBroadcast | EPCGAttributeAccessorFlags::AllowConstructible;

		const int32 NumberOfElements = OutputKeys->GetNum();
		constexpr int32 ChunkSize = 256;

		TArray<OutputType, TInlineAllocator<ChunkSize>> TempValues;
		TempValues.SetNum(ChunkSize);

		const int32 NumberOfIterations = (NumberOfElements + ChunkSize - 1) / ChunkSize;

		for (int32 i = 0; i < NumberOfIterations; ++i)
		{
			const int32 StartIndex = i * ChunkSize;
			const int32 Range = FMath::Min(NumberOfElements - StartIndex, ChunkSize);
			TArrayView<OutputType> View(TempValues.GetData(), Range);

			if (!InputAccessor->GetRange<OutputType>(View, StartIndex, *InputKeys, Flags)
				|| !OutputAccessor->SetRange<OutputType>(View, StartIndex, *OutputKeys, Flags))
			{
				PCGE_LOG_C(Error, GraphAndLog, Context, LOCTEXT("ConversionFailed", "Source attribute/property cannot be converted to target attribute/property"));
				return false;
			}
		}

		return true;
	};

	if (!PCGMetadataAttribute::CallbackWithRightType(OutputAccessor->GetUnderlyingType(), Operation))
	{
		PCGE_LOG(Error, GraphAndLog, LOCTEXT("ErrorGettingSettingValues", "Error while getting/setting values"));
		return true;
	}

	AppendOutputData();

	return true;
}

#undef LOCTEXT_NAMESPACE
